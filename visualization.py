import networkx as nx
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from google.cloud import bigquery
import argparse
import os
from plotly.subplots import make_subplots
import matplotlib.cm as cm
import matplotlib.colors as mcolors

def visualize_3d_graph_from_bigquery(project_id="defi-sentinal", dataset_id="visualization_dataset", 
                                    vertices_table="graph_vertices", edges_table="graph_edges",
                                    limit=500, output_file="dex_network_3d_visualization.html"):
    """
    Create an interactive 3D visualization of graph data from BigQuery tables
    
    Args:
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        vertices_table: Name of the vertices table
        edges_table: Name of the edges table
        limit: Maximum number of vertices to visualize
        output_file: Name of the output HTML file
    """
    print(f"Connecting to BigQuery project: {project_id}")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Query vertices with limit
    print(f"Retrieving top {limit} vertices by volume...")
    vertices_query = f"""
    SELECT id, total_volume, total_transactions, symbol, 
           COALESCE(total_buy_volume, 0) as total_buy_volume,
           COALESCE(total_sell_volume, 0) as total_sell_volume
    FROM `{project_id}.{dataset_id}.{vertices_table}`
    ORDER BY total_volume DESC
    LIMIT {limit}
    """
    
    vertices_df = client.query(vertices_query).to_dataframe()
    vertex_ids = vertices_df['id'].tolist()
    
    # Query edges connecting these vertices
    print("Retrieving edges between selected vertices...")
    edges_query = f"""
    SELECT src, dst, weight, total_buy_value, total_sell_value, 
           transaction_count, avg_gas_price
    FROM `{project_id}.{dataset_id}.{edges_table}`
    WHERE src IN UNNEST(@vertices) AND dst IN UNNEST(@vertices)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("vertices", "STRING", vertex_ids),
        ]
    )
    
    edges_df = client.query(edges_query, job_config=job_config).to_dataframe()
    
    print(f"Retrieved {len(vertices_df)} vertices and {len(edges_df)} edges")
    
    # Create NetworkX graph
    G = nx.DiGraph()
    
    # Add nodes with attributes
    for _, row in vertices_df.iterrows():
        G.add_node(row['id'], 
                  volume=row['total_volume'], 
                  transactions=row['total_transactions'],
                  symbol=row['symbol'],
                  buy_volume=row['total_buy_volume'],
                  sell_volume=row['total_sell_volume'])
    
    # Add edges with attributes
    for _, row in edges_df.iterrows():
        G.add_edge(row['src'], row['dst'], 
                  weight=row['weight'],
                  buy_value=row['total_buy_value'],
                  sell_value=row['total_sell_value'],
                  transaction_count=row['transaction_count'],
                  avg_gas_price=row['avg_gas_price'])
    
    print(f"Created graph with {len(G.nodes())} nodes and {len(G.edges())} edges")
    
    # Generate 3D layout
    print("Computing 3D layout (this may take a while for large graphs)...")
    try:
        # Try using a 3D spring layout
        pos_3d = nx.spring_layout(G, dim=3, k=0.3, iterations=100, seed=42)
    except:
        # Fallback: Generate random 3D positions
        pos_3d = {node: np.random.rand(3) for node in G.nodes()}
    
    # Create node data for 3D visualization
    node_x = [pos_3d[node][0] for node in G.nodes()]
    node_y = [pos_3d[node][1] for node in G.nodes()]
    node_z = [pos_3d[node][2] for node in G.nodes()]
    
    # Scale node sizes based on transaction volume (logarithmic scale)
    volumes = [G.nodes[node].get('volume', 1) for node in G.nodes()]
    min_size, max_size = 5, 30
    node_sizes = [(np.log1p(volume) / np.log1p(max(volumes))) * (max_size - min_size) + min_size for volume in volumes]
    
    # Color nodes by their token symbol
    symbols = [G.nodes[node].get('symbol', '') for node in G.nodes()]
    unique_symbols = list(set([s for s in symbols if s]))
    
    # Create color map with better contrasting colors using a categorical colormap
    viridis = cm.get_cmap('tab20', len(unique_symbols)) 
    color_map = {}
    for i, symbol in enumerate(unique_symbols):
        color_map[symbol] = i
    
    # Get normalized numeric values for colors
    node_color_values = [color_map.get(symbol, 0) if symbol else 0 for symbol in symbols]
    
    # Convert to actual colors for custom hover text
    rgb_colors = [viridis(i/len(unique_symbols)) for i in range(len(unique_symbols))]
    hex_colors = [mcolors.rgb2hex(rgb[:3]) for rgb in rgb_colors]
    color_mapping = {symbol: hex_colors[i] for i, symbol in enumerate(unique_symbols)}
    
    # Create edge trace for 3D visualization
    edge_traces = []
    
    # Calculate the maximum edge weight for normalization
    max_weight = max([G.edges[edge].get('weight', 1) for edge in G.edges()]) if G.edges else 1
    
    # Create a separate trace for each edge to enable individual hover info
    for edge in G.edges():
        x0, y0, z0 = pos_3d[edge[0]]
        x1, y1, z1 = pos_3d[edge[1]]
        
        # Calculate edge color based on transaction value
        edge_weight = G.edges[edge].get('weight', 1)
        edge_color = f'rgba(150, 150, 150, {min(0.8, max(0.1, edge_weight / max_weight))})'
        
        # Adjust width based on weight
        edge_width = 1 + 3 * (edge_weight / max_weight)
        
        # Create hover text
        hover_text = (f"Source: {edge[0][:8]}...{edge[0][-6:]}<br>"
                     f"Target: {edge[1][:8]}...{edge[1][-6:]}<br>"
                     f"Transactions: {G.edges[edge].get('transaction_count', 0)}<br>"
                     f"Buy Value: {G.edges[edge].get('buy_value', 0):.2f}<br>"
                     f"Sell Value: {G.edges[edge].get('sell_value', 0):.2f}")
        
        edge_trace = go.Scatter3d(
            x=[x0, x1], y=[y0, y1], z=[z0, z1],
            mode='lines',
            line=dict(color=edge_color, width=edge_width),
            hoverinfo='text',
            text=hover_text,
            showlegend=False
        )
        edge_traces.append(edge_trace)
    
    # Create node trace with hover information
    node_info = []
    for node in G.nodes():
        symbol = G.nodes[node].get('symbol', 'Unknown')
        volume = G.nodes[node].get('volume', 0)
        buy_vol = G.nodes[node].get('buy_volume', 0)
        sell_vol = G.nodes[node].get('sell_volume', 0)
        transactions = G.nodes[node].get('transactions', 0)
        
        info = (f"Address: {node[:10]}...{node[-8:]}<br>"
                f"Token: {symbol}<br>"
                f"Volume: {volume:.2f}<br>"
                f"Buy Volume: {buy_vol:.2f}<br>"
                f"Sell Volume: {sell_vol:.2f}<br>"
                f"Transactions: {transactions}")
        node_info.append(info)
    
    # Create node trace for 3D
    node_trace = go.Scatter3d(
        x=node_x, y=node_y, z=node_z,
        mode='markers',
        marker=dict(
            size=node_sizes,
            color=node_color_values,
            colorscale='Turbo',  # More visually distinct in 3D
            opacity=0.9,
            colorbar=dict(
                title='Token Type',
                tickvals=list(range(len(unique_symbols))),
                ticktext=unique_symbols,
                lenmode='fraction',
                len=0.75
            ),
            line=dict(width=1, color='#000000')
        ),
        text=node_info,
        hoverinfo='text'
    )
    
    # Create the figure with all traces
    print("Creating 3D visualization...")
    
    # Combine all traces
    data = [node_trace] + edge_traces
    
    # Creating layout with fixed title_font instead of titlefont
    fig = go.Figure(
        data=data,
        layout=go.Layout(
            title='3D DEX Trading Network',
            title_font=dict(size=16),  # FIXED: Changed from titlefont to title_font
            showlegend=False,
            scene=dict(
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
                zaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
                bgcolor='rgba(240, 240, 240, 0.5)'
            ),
            margin=dict(b=0, l=0, r=0, t=30),
            paper_bgcolor='white',
            annotations=[
                dict(
                    showarrow=False,
                    x=0.01,
                    y=0.01,
                    xref='paper',
                    yref='paper',
                    text='DEX Transaction Network',
                    font=dict(size=14)
                )
            ],
        )
    )
    
    # Add advanced 3D camera options
    fig.update_layout(
        scene_camera=dict(
            up=dict(x=0, y=0, z=1),
            center=dict(x=0, y=0, z=0),
            eye=dict(x=1.5, y=1.5, z=1.5)
        ),
        scene_dragmode='orbit',
        updatemenus=[
            dict(
                type='buttons',
                showactive=False,
                buttons=[
                    dict(
                        label='Reset Camera',
                        method='relayout',
                        args=['scene.camera', dict(
                            up=dict(x=0, y=0, z=1),
                            center=dict(x=0, y=0, z=0),
                            eye=dict(x=1.5, y=1.5, z=1.5)
                        )]
                    ),
                    dict(
                        label='Top View',
                        method='relayout',
                        args=['scene.camera', dict(
                            up=dict(x=0, y=1, z=0),
                            center=dict(x=0, y=0, z=0),
                            eye=dict(x=0, y=0, z=2)
                        )]
                    ),
                    dict(
                        label='Side View',
                        method='relayout',
                        args=['scene.camera', dict(
                            up=dict(x=0, y=0, z=1),
                            center=dict(x=0, y=0, z=0),
                            eye=dict(x=2, y=0, z=0)
                        )]
                    )
                ],
                direction='down',
                pad={'r': 10, 't': 10},
                x=0.1,
                y=0.1,
                xanchor='left',
                yanchor='top'
            )
        ]
    )
    
    # Set up sliders for filtering
    # Create a slider to filter nodes by volume
    min_volume = min(volumes)
    max_volume = max(volumes)
    
    # Add slider for node filter by volume
    steps = []
    for step in np.linspace(0, 1, 10):
        threshold = min_volume + step * (max_volume - min_volume)
        
        # Calculate visibility
        node_visibility = [volume >= threshold for volume in volumes]
        
        # Update edge visibility based on connected nodes
        edge_visibility_list = []
        for edge in G.edges():
            source_volume = G.nodes[edge[0]].get('volume', 0)
            target_volume = G.nodes[edge[1]].get('volume', 0)
            edge_visibility_list.append(source_volume >= threshold and target_volume >= threshold)
        
        step_label = f"Volume ≥ {threshold:.2e}"
        
        # Create visibility update dict for all traces
        visibility_updates = {}
        # Update node trace
        visibility_updates['node_trace'] = {'visible': True, 'opacity': 0.9}
        
        steps.append(dict(
            method='update',
            label=step_label,
            args=[{'visible': [True] + edge_visibility_list}]
        ))
    
    # Save interactive visualization to HTML
    config = {
        'displayModeBar': True,
        'modeBarButtonsToAdd': ['resetCameraLastSave3d']
    }
    
    fig.update_layout(
        scene=dict(
            aspectmode='data'  # or 'cube' for equal aspect ratios
        )
    )
    
    # Write to HTML with full ModeBar
    fig.write_html(
        output_file,
        include_plotlyjs='cdn',
        include_mathjax='cdn',
        full_html=True,
        config=config
    )
    
    print(f"3D visualization saved to {output_file}")
    
    # Print statistics about the graph
    print("\nGraph Statistics:")
    print(f"Number of nodes: {len(G.nodes())}")
    print(f"Number of edges: {len(G.edges())}")
    
    # Find hubs (high out-degree nodes)
    hub_nodes = sorted(G.out_degree(), key=lambda x: x[1], reverse=True)[:10]
    print("\nTop 10 hub nodes (highest out-degree):")
    for node, degree in hub_nodes:
        symbol = G.nodes[node].get('symbol', 'Unknown')
        volume = G.nodes[node].get('volume', 0)
        print(f"Address: {node[:10]}...{node[-8:]} | Symbol: {symbol} | Out-degree: {degree} | Volume: {volume:.2f}")
    
    # Find authorities (high in-degree nodes)
    authority_nodes = sorted(G.in_degree(), key=lambda x: x[1], reverse=True)[:10]
    print("\nTop 10 authority nodes (highest in-degree):")
    for node, degree in authority_nodes:
        symbol = G.nodes[node].get('symbol', 'Unknown')
        volume = G.nodes[node].get('volume', 0)
        print(f"Address: {node[:10]}...{node[-8:]} | Symbol: {symbol} | In-degree: {degree} | Volume: {volume:.2f}")
    
    return G, fig

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Create 3D Visualization of DEX Trading Network')
    parser.add_argument('--project', type=str, default='defi-sentinal', help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='visualization_dataset', help='BigQuery dataset ID')
    parser.add_argument('--vertices', type=str, default='graph_vertices', help='Vertices table name')
    parser.add_argument('--edges', type=str, default='graph_edges', help='Edges table name')
    parser.add_argument('--limit', type=int, default=500, help='Maximum number of nodes to visualize')
    parser.add_argument('--output', type=str, default='dex_network_3d_visualization.html', help='Output HTML file')
    
    args = parser.parse_args()
    
    # Call the 3D visualization function
    G, fig = visualize_3d_graph_from_bigquery(
        project_id=args.project,
        dataset_id=args.dataset,
        vertices_table=args.vertices,
        edges_table=args.edges,
        limit=args.limit,
        output_file=args.output
    )
    
    print(f"\nVisualization completed! Open {args.output} in a web browser to view the interactive 3D graph.")
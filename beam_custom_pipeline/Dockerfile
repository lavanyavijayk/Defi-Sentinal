FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set paths for requirements and pipeline
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/rules.py"

RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip
    

# Optional: Speed up job submission with older Beam versions
ENV PIP_NO_DEPS=True

# Copy source code to image
COPY . /template

RUN pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

# This launcher uses the environment variables above to launch the pipeline
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]


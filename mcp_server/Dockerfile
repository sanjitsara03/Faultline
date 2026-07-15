# MintMCP hosted connector image: serves MCP over streamable HTTP at /mcp:8000
# (their platform's preferred transport). linux/amd64 per their requirements.
FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .

# DATABASE_URL is NOT baked in: MintMCP injects it from connector config.
ENV MCP_TRANSPORT=http
EXPOSE 8000
CMD ["python", "server.py"]

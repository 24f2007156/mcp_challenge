import asyncio
import json
import hashlib
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Allow cross-origin requests from the grader
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store queues for active grader connections
sessions = {}

@app.get("/sse")
async def sse(request: Request):
    """Establishes the MCP Server-Sent Events (SSE) connection."""
    session_id = str(uuid.uuid4())
    queue = asyncio.Queue()
    sessions[session_id] = queue
    
    async def event_generator():
        try:
            # Tell the client to send POST requests to /messages?session_id=...
            yield f"event: endpoint\ndata: /messages?session_id={session_id}\n\n"
            
            while True:
                if await request.is_disconnected():
                    break
                # Wait for JSON-RPC messages pushed by the POST endpoint
                message = await queue.get()
                yield f"event: message\ndata: {json.dumps(message)}\n\n"
        finally:
            sessions.pop(session_id, None)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/messages")
async def messages(request: Request, session_id: str):
    """Receives JSON-RPC messages and HTTP headers from the client."""
    if session_id not in sessions:
        return Response(status_code=404, content="Session not found")
        
    queue = sessions[session_id]
    payload = await request.json()
    
    # Extract the custom challenge header!
    challenge = request.headers.get("X-Exam-Challenge")
    
    method = payload.get("method")
    req_id = payload.get("id")
    
    # 1. Handle Initialization
    if method == "initialize":
        await queue.put({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "exam-server", "version": "1.0"}
            }
        })
        
    # 2. Expose the tool schema
    elif method == "tools/list":
        await queue.put({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [{
                    "name": "solve_challenge",
                    "description": "Solves the exam challenge",
                    "inputSchema": {"type": "object", "properties": {}}
                }]
            }
        })
        
    # 3. Execute the tool
    elif method == "tools/call":
        params = payload.get("params", {})
        if params.get("name") == "solve_challenge":
            if challenge:
                # Format string exactly as requested
                email = "24f2007156@ds.study.iitm.ac.in"
                raw_str = f"{challenge}:{email}"
                
                # Hash it and slice first 16 characters
                hash_val = hashlib.sha256(raw_str.encode('utf-8')).hexdigest()
                text_val = hash_val[:16]
            else:
                text_val = "Error: Missing X-Exam-Challenge header"
            
            await queue.put({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": text_val}]
                }
            })
            
    # Standard ping fallback
    elif method == "ping":
        await queue.put({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {}
        })
        
    # The MCP protocol requires a 202 Accepted status for HTTP POSTs
    return Response(status_code=202)
import asyncio
import json
import hashlib
import uuid
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok", "message": "MCP Server is running! Please point your MCP Client or grading portal to the /mcp endpoint."}

sessions = {}

async def handle_sse_connection(request: Request):
    session_id = str(uuid.uuid4())
    queue = asyncio.Queue()
    sessions[session_id] = queue
    
    async def event_generator():
        try:
            post_url = f"{request.url.path}?session_id={session_id}"
            yield f"event: endpoint\ndata: {post_url}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                message = await queue.get()
                yield f"event: message\ndata: {json.dumps(message)}\n\n"
        finally:
            sessions.pop(session_id, None)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

async def handle_messages(request: Request, session_id: str):
    if session_id not in sessions:
        return Response(status_code=404, content="Session not found")
        
    queue = sessions[session_id]
    payload = await request.json()
    challenge = request.headers.get("X-Exam-Challenge")
    
    method = payload.get("method")
    req_id = payload.get("id")
    
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
    elif method == "tools/call":
        params = payload.get("params", {})
        if params.get("name") == "solve_challenge":
            if challenge:
                email = "24f2007156@ds.study.iitm.ac.in"
                raw_str = f"{challenge}:{email}"
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
    elif method == "ping":
        await queue.put({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {}
        })
        
    return Response(status_code=202)

@app.get("/sse")
async def sse_get(request: Request):
    return await handle_sse_connection(request)

@app.get("/mcp")
async def mcp_get(request: Request):
    return await handle_sse_connection(request)

@app.post("/mcp")
@app.post("/sse")
@app.post("/messages")
async def messages_post(request: Request):
    session_id = request.query_params.get("session_id")
    if not session_id:
        if not sessions:
            return Response(status_code=400, content="No active session found")
        session_id = list(sessions.keys())[0]
    return await handle_messages(request, session_id)
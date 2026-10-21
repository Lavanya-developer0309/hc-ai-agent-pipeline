"""Local LLM (Ollama) with tool calling. Free, and nothing leaves the Mac."""
import json
import os
import urllib.error
import urllib.request

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
MODEL = os.getenv("AGENT_MODEL", "qwen3:8b")


def chat(messages, tools):
    """One model turn. Returns the assistant message (content and/or tool_calls)."""
    body = json.dumps({
        "model": MODEL, "messages": messages, "tools": tools, "stream": False, "think": False,
        "options": {"temperature": 0, "num_ctx": 8192},
    }).encode()
    req = urllib.request.Request(f"{OLLAMA_URL}/api/chat", body, {"Content-Type": "application/json"})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=600).read())["message"]
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError(f"Ollama is not reachable at {OLLAMA_URL} - start it with: ollama serve ({e})") from None

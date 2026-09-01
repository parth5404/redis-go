import asyncio
import json
import re
from typing import Any, Dict, List
import pydantic
from mcp.server.fastmcp import FastMCP, Context
import mcp.types as types
from mcp.client.stdio import stdio_client, StdioServerParameters
from mcp.client.session import ClientSession

mcp_app = FastMCP("Redis ReAct Agent")

# Global state for the Go subprocess MCP client
go_session: ClientSession | None = None
go_client_ctx: Any = None
go_read: Any = None
go_write: Any = None

@mcp_app.on_startup
async def start_go_server():
    global go_session, go_client_ctx, go_read, go_write
    print("Starting Go MCP server as a subprocess...")
    server_params = StdioServerParameters(
        command="go",
        args=["run", "main.go", "--mcp"],
        cwd=".."
    )
    go_client_ctx = stdio_client(server_params)
    go_read, go_write = await go_client_ctx.__aenter__()
    go_session = ClientSession(go_read, go_write)
    await go_session.__aenter__()
    await go_session.initialize()
    print("Go MCP server initialized successfully.")

@mcp_app.on_shutdown
async def stop_go_server():
    global go_session, go_client_ctx
    if go_session:
        await go_session.__aexit__(None, None, None)
    if go_client_ctx:
        await go_client_ctx.__aexit__(None, None, None)
    print("Go MCP server shut down.")

async def call_go_tool(tool_name: str, args: Dict[str, Any]) -> str:
    """Helper to call the Go MCP server tools."""
    if not go_session:
        raise RuntimeError("Go session not initialized")
    result = await go_session.call_tool(tool_name, arguments=args)
    if result.isError:
        return f"Error: {result.content[0].text}"
    return result.content[0].text

@mcp_app.tool()
async def manage_redis_data(instruction: str, ctx: Context) -> str:
    """
    An agent that processes natural language instructions to perform CRUD operations on Redis.
    Use this tool instead of calling individual redis_set/redis_get tools.
    """
    
    system_prompt = (
        "You are an agent managing a Redis database. "
        "You have access to two tools. To use a tool, you MUST output a JSON block exactly matching this format:\n"
        "```json\n"
        "{\n"
        "  \"tool\": \"tool_name\",\n"
        "  \"arguments\": {\"arg1\": \"value1\"}\n"
        "}\n"
        "```\n\n"
        "Available Tools:\n"
        "1. redis_set\n"
        "   Description: Set a key-value pair in Redis\n"
        "   Arguments: key (string), value (string)\n"
        "2. redis_get\n"
        "   Description: Get a value by key from Redis\n"
        "   Arguments: key (string)\n\n"
        "Analyze the user's instruction and execute the necessary tools to fulfill it. "
        "You can only use ONE tool at a time. After you use a tool, you will receive the result. "
        "When you are finished and have completed the instruction, return a final summary of what you did WITHOUT a JSON tool block. "
        "Always explain what you are doing before emitting a JSON block."
    )
    
    messages = [
        types.SamplingMessage(
            role="user",
            content=types.TextContent(type="text", text=instruction)
        )
    ]
    
    max_steps = 6
    final_response = "Agent failed to complete the task."
    
    for step in range(max_steps):
        try:
            # Sample the client's LLM
            response = await ctx.session.create_message(
                messages=messages,
                systemPrompt=system_prompt,
                includeContext="none",
                maxTokens=1000,
            )
        except Exception as e:
            return f"Failed to sample LLM (does the client support sampling?): {e}"
        
        # Parse the assistant's response
        assistant_text = ""
        # The content from Sampling can be a TextContent or an ImageContent
        if getattr(response, "content", None):
            if isinstance(response.content, types.TextContent):
                assistant_text = response.content.text
            elif isinstance(response.content, str):
                assistant_text = response.content
            # handle other types if needed
        else:
            assistant_text = str(response)
        
        # Append assistant message to history
        messages.append(types.SamplingMessage(
            role="assistant",
            content=types.TextContent(type="text", text=assistant_text)
        ))
        
        # Look for a JSON block containing tool usage
        match = re.search(r"```json\s*(\{.*?\})\s*```", assistant_text, re.DOTALL)
        if match:
            tool_call_str = match.group(1)
            try:
                tool_call = json.loads(tool_call_str)
                tool_name = tool_call.get("tool")
                tool_args = tool_call.get("arguments", {})
                
                # Execute tool
                tool_result = await call_go_tool(tool_name, tool_args)
                
                # Append tool result to history as user message
                messages.append(types.SamplingMessage(
                    role="user",
                    content=types.TextContent(type="text", text=f"Tool '{tool_name}' result: {tool_result}")
                ))
            except json.JSONDecodeError:
                messages.append(types.SamplingMessage(
                    role="user",
                    content=types.TextContent(type="text", text="Error: Invalid JSON format. Please output valid JSON inside ```json``` block.")
                ))
            except Exception as e:
                messages.append(types.SamplingMessage(
                    role="user",
                    content=types.TextContent(type="text", text=f"Error executing tool: {str(e)}")
                ))
        else:
            # No tool call found, meaning the agent is done!
            final_response = assistant_text
            break
            
    return final_response

if __name__ == "__main__":
    mcp_app.run(transport="stdio")

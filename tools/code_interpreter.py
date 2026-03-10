import os
try:
    from e2b_code_interpreter import Sandbox
except ImportError:
    Sandbox = None

class CodeInterpreterTool:
    def __init__(self, api_key=None):
        self.api_key = api_key
        self.sandbox = None

    def start(self):
        if Sandbox is None:
            raise ImportError("e2b_code_interpreter is not installed. Please install it with `pip install e2b-code-interpreter`.")
        
        if self.api_key:
            os.environ["E2B_API_KEY"] = self.api_key
            
        if not self.sandbox:
            # If api_key is not provided, it will try to read from os.environ["E2B_API_KEY"]
            self.sandbox = Sandbox.create()

    def stop(self):
        if self.sandbox:
            self.sandbox.kill()
            self.sandbox = None

    def execute_code(self, code: str):
        if not self.sandbox:
            try:
                self.start()
            except Exception as e:
                return f"Error starting sandbox: {str(e)}. Make sure E2B_API_KEY is set in your environment variables."
        
        try:
            execution = self.sandbox.run_code(code)
            output = []
            
            # Stdout
            if execution.logs.stdout:
                output.append("--- Stdout ---")
                output.append("\n".join(execution.logs.stdout))
            
            # Stderr
            if execution.logs.stderr:
                output.append("--- Stderr ---")
                output.append("\n".join(execution.logs.stderr))
            
            # Results (return values, charts, etc.)
            if execution.results:
                output.append("--- Results ---")
                for result in execution.results:
                    if result.text:
                        output.append(str(result.text))
                    # Handle charts/plots if necessary (e.g. mention them)
                    if hasattr(result, 'png') and result.png:
                        output.append("[Chart/Image generated]")
                    if hasattr(result, 'jpeg') and result.jpeg:
                        output.append("[Chart/Image generated]")
                    # Handle other formats if needed

            # Execution Error
            if execution.error:
                output.append("--- Execution Error ---")
                output.append(f"{execution.error.name}: {execution.error.value}")
                if execution.error.traceback:
                    output.append(execution.error.traceback)
            
            return "\n".join(output) if output else "Code executed successfully (no output)."
            
        except Exception as e:
            return f"System Error executing code: {str(e)}"

CODE_INTERPRETER_SCHEMA = {
    "type": "function",
    "function": {
        "name": "execute_python_code",
        "description": "Execute Python code in a secure sandbox environment. Use this to perform data analysis (pandas, numpy), math calculations, or run Python scripts. The environment preserves state between calls within the same session.",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The Python code to execute."
                }
            },
            "required": ["code"]
        }
    }
}

from flask import Flask, jsonify, request, render_template
from flask_socketio import SocketIO, emit
import json
from jupyter_client import KernelManager
import os
from queue import Empty as QueueEmpty
import logging
import threading

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the root logger level to the lowest level needed

# Debug file handler
debug_handler = logging.FileHandler('app_debug.log')
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(debug_formatter)

# Info file handler
info_handler = logging.FileHandler('app_info.log')
info_handler.setLevel(logging.INFO)
info_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
info_handler.setFormatter(info_formatter)

# Add handlers to the logger
logger.addHandler(debug_handler)
logger.addHandler(info_handler)


app = Flask(__name__)
kernel_manager = None
socketio = SocketIO(app, cors_allowed_origins="*")

session_kernels = {}
session_threads = {}
session_stop_flags = {}

@app.route('/')
def home():
    """Serve the main page (index.html)."""
    return render_template('index.html')

@app.route('/load_notebook', methods=['POST'])
def load_notebook():
    """Load a Jupyter Notebook and return the code cells."""
    # file = request.files['notebook']
    filename = "example.ipynb"
    notebook = json.load(open(filename, 'r'))
    notebook_cells = [cell['source'] for cell in notebook['cells'] if cell['cell_type'] == 'code']
    return jsonify({'cells': notebook_cells})


@socketio.on('execute_cell')
def handle_execute_cell(data):
    """Execute a code cell and return the result."""
    session_id = request.sid  # Get the session ID
    logger.info(f"[{session_id}]  Executing cell: {data['cell']}")
    # Ensure a KernelManager exists for this session
    if session_id not in session_kernels:
        logger.info(f"[{session_id}] Starting kernel for new session...")
        kernel_manager = KernelManager()
        kernel_manager.start_kernel()
        kernel_client = kernel_manager.client()
        session_kernels[session_id] = {
            'kernel_manager': kernel_manager,
            'kernel_client': kernel_client,
        }
        kernel_client.start_channels()

        # Create a stop flag for the session
        stop_flag = threading.Event()
        session_stop_flags[session_id] = stop_flag

        # Start a background thread for this session
        thread = threading.Thread(target=kernel_message_listener, args=(session_id, stop_flag))
        thread.daemon = True
        session_threads[session_id] = thread
        thread.start()

    kernel_manager = session_kernels[session_id]['kernel_manager']
    kernel_client = session_kernels[session_id]['kernel_client']

    code = "".join(data['cell'])
    logger.info(f"[{session_id}] Executing code: {code}")
    last_line = code.split('\n')[-1]
    code = "\n".join(code.split('\n')[:-1]) + f"\ndisplay({last_line})"
    msg_id = kernel_client.execute(code)
    logger.info(f"[{session_id}] Done executing code.")
    locals_code_str = """'\\n'.join([f'{_k}: {_v}' for _k, _v in locals().items() if isinstance(_v, (list, dict, int, str, bool, float, tuple, set)) and not _k.startswith('_') and not _k in ['Out', 'In']])"""
    locals_code_str = "display({'locals_display': " + locals_code_str + "})"
    logger.info(f"[{session_id}] Executing locals code: {locals_code_str}")
    _ = kernel_client.execute(locals_code_str)

def kernel_message_listener(session_id, stop_flag):
    """Background thread to listen for kernel messages."""
    kernel_client = session_kernels[session_id]['kernel_client']
    logger.info(f"[{session_id}] Starting kernel message listener...")
    while not stop_flag.is_set():
        try:
            logger.info(f"[{session_id}] Waiting for message...")
            msg = kernel_client.get_iopub_msg(timeout=1)
            with open("messages.csv", "a") as f:
                # Convert any non JSON-serializable objects to strings
                f.write(f"{json.dumps(msg, default=str, ensure_ascii=True)}\n")
            logger.debug(f"[{session_id}] Received message: {msg}")

            def emit_message_to_client(session_id, msg):
                """Emit the message to the client."""
                msg_type = msg['msg_type']
                content = msg['content']
                if msg_type == 'execute_result':
                    logger.info(f"[{session_id}] execute_result received.")
                    cnt = content['data']['text/plain']
                    socketio.emit('execute_result', {'output': cnt}, to=session_id)
                    logger.info(f"[{session_id}] execute_result: {cnt}")
                elif msg_type == 'stream':
                    socketio.emit('stream', {'output': content['text']}, to=session_id)
                    logger.info(f"[{session_id}] Stream: {content['text']}")
                elif msg_type == 'error':
                    traceback = '\n'.join(content['traceback'])
                    socketio.emit('error', {'output': traceback}, to=session_id)
                    logger.info(f"[{session_id}] Error: {traceback}")
                elif msg_type == 'execute_request':
                    logger.info(f"[{session_id}] execute_request received.")
                    if "execution_state" in content:
                        execution_state = content['execution_state']
                        logger.info(f"[{session_id}] Execution state:  {execution_state}")
                elif msg_type == 'display_data':
                    if "data" in content:
                        data = content['data']
                        if "text/html" in data:
                            cnt = data['text/html']
                        elif "text/plain" in data:
                            cnt = data['text/plain']
                            if 'locals_display' in cnt:
                                cnt = cnt.replace("{'locals_display': ", "")[:-1][1:-1]
                                socketio.emit('locals', {'output': cnt}, to=session_id)
                                logger.info(f"[{session_id}] locals: {cnt}")
                                return
                        if "image/png" in data:
                            cnt = data['image/png']
                            socketio.emit('image', {'output': cnt}, to=session_id)
                            return
                        logger.info(f"[{session_id}] execute_request: {cnt}")
                        if cnt == "None":
                            return
                        socketio.emit('execute_result', {'output': cnt}, to=session_id)
            emit_message_to_client(session_id, msg)
        except QueueEmpty:
            continue  # No message; keep listening
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            logger.error(f"[{session_id}] Exception in listener thread: {trace}")
            break
    logger.info(f"[{session_id}] Exiting listener thread...")


@socketio.on('disconnect')
def handle_disconnect():
    """Clean up resources when a user disconnects."""
    session_id = request.sid
    logger.info(f"[{session_id}] User disconnected")

    # Stop the thread
    if session_id in session_stop_flags:
        session_stop_flags[session_id].set()  # Signal the thread to stop
        session_threads[session_id].join()   # Wait for the thread to finish

    # Clean up session resources
    if session_id in session_kernels:
        kernel_manager = session_kernels[session_id]['kernel_manager']
        kernel_manager.shutdown_kernel()
        del session_kernels[session_id]

    if session_id in session_threads:
        del session_threads[session_id]

    if session_id in session_stop_flags:
        del session_stop_flags[session_id]
    logger.info(f"[{session_id}] Session resources cleaned up")

if __name__ == "__main__":
    logger.info("================================")
    logger.info("Starting Flask app...")
    socketio.run(app, debug=True)

"""
execute_notebook.py
Executes the code cells of data_profiling.ipynb sequentially in a shared
namespace and stores real execution outputs, execution counts, and text
streams inside the notebook JSON.
"""
import io
import json
import sys
from contextlib import redirect_stdout, redirect_stderr

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend to prevent GUI popups
import matplotlib.pyplot as plt

NOTEBOOK_PATH = "notebooks/data_profiling.ipynb"

def run_notebook():
    with open(NOTEBOOK_PATH, "r", encoding="utf-8") as f:
        nb = json.load(f)

    # Provide display as print
    shared_globals = {
        "display": print,
        "plt": plt,
    }
    code_idx = 1

    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            code = "".join(cell["source"])
            # Remove plt.show() to prevent blocking
            code_clean = code.replace("plt.show()", "plt.close()")

            f_out = io.StringIO()
            f_err = io.StringIO()
            cell["execution_count"] = code_idx
            try:
                with redirect_stdout(f_out), redirect_stderr(f_err):
                    exec(code_clean, shared_globals)
                out_text = f_out.getvalue()
                err_text = f_err.getvalue()

                outputs = []
                if out_text:
                    outputs.append({
                        "name": "stdout",
                        "output_type": "stream",
                        "text": [line + "\n" for line in out_text.splitlines()]
                    })
                if err_text:
                    outputs.append({
                        "name": "stderr",
                        "output_type": "stream",
                        "text": [line + "\n" for line in err_text.splitlines()]
                    })
                cell["outputs"] = outputs
                print(f"✓ Cell {code_idx} executed successfully ({len(out_text.splitlines())} stdout lines)")
            except Exception as e:
                print(f"✗ Cell {code_idx} error: {e}")
                cell["outputs"] = [{
                    "ename": type(e).__name__,
                    "evalue": str(e),
                    "output_type": "error",
                    "traceback": [str(e)]
                }]
            code_idx += 1

    with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=2, ensure_ascii=False)
    print("✓ Saved executed notebook with populated outputs to:", NOTEBOOK_PATH)

if __name__ == "__main__":
    run_notebook()

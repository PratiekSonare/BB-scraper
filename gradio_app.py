import gradio as gr
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import scraper
import zipfile
import time
from csv_files import csv_files


def run_stream_and_display():
    for log, progress, scraped in scraper.run_scraper_stream():

        # Format the file list with ✅
        updated_list = ""
        for fname in csv_files:
            if fname in scraped:
                updated_list += f"✅ {fname}\n"
            else:
                updated_list += f"▫️ {fname}\n"

        yield log, progress, updated_list.strip()


with gr.Blocks(title="BigBasket Web Scraper") as demo:
    gr.Markdown("# 🛒 BigBasket Web Scraper GUI")
    gr.Markdown("Click **Start Scraping** to begin. Logs will appear below, and progress will update in real-time.")

    with gr.Row():
        with gr.Column(scale=2):
            start_btn = gr.Button("Start Scraping")
            logbox = gr.Textbox(label="Live Logs", lines=25, interactive=False)
        with gr.Column(scale=2):
            file_status = gr.Textbox(label="Scraped CSVs", lines=10, interactive=False)
            # download = gr.File(label="📥 Download Results (Auto)", visible=True)
            progress = gr.Slider(minimum=0, maximum=1, step=0.01, label="Progress", interactive=False)


    start_btn.click(fn=run_stream_and_display, outputs=[logbox, progress, file_status])

port = int(os.environ.get("PORT", 7860))
demo.launch(server_name="0.0.0.0", server_port=port)


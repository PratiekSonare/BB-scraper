import gradio as gr
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import scraper
import zipfile
import time
from csv_files import csv_files

stop_requested = False

def run_stream_and_display():
    global stop_requested
    stop_requested = False  # Reset at the beginning

    for log, progress, scraped in scraper.run_scraper_stream():
        if stop_requested:
            break

        updated_list = ""
        for fname in csv_files:
            if fname in scraped:
                updated_list += f"✅ {fname}\n"
            else:
                updated_list += f"▫️ {fname}\n"

        yield log, progress, updated_list.strip()


def stop_scraping():
    global stop_requested
    stop_requested = True
    return "⛔ Scraping stopped by user.", 0, ""

with gr.Blocks(title="BigBasket Web Scraper") as demo:
    gr.Markdown("# 🛒 BigBasket Web Scraper GUI")
    gr.Markdown("Click **Start Scraping** to begin. Logs will appear below, and progress will update in real-time.")

    with gr.Row():
        with gr.Column(scale=2):
            start_btn = gr.Button("Start Scraping")
            stop_btn = gr.Button("🛑 Stop Scraping", variant="stop")  # New Stop button
            logbox = gr.Textbox(label="Live Logs", lines=25, interactive=False)
        with gr.Column(scale=2):
            file_status = gr.Textbox(label="Scraped CSVs", lines=10, interactive=False)
            progress = gr.Slider(minimum=0, maximum=1, step=0.01, label="Progress", interactive=False)

    start_btn.click(fn=run_stream_and_display, outputs=[logbox, progress, file_status])
    stop_btn.click(fn=stop_scraping, outputs=[logbox, progress, file_status])



    start_btn.click(fn=run_stream_and_display, outputs=[logbox, progress, file_status])

port = int(os.environ.get("PORT", 8000))
demo.launch(server_name="0.0.0.0", server_port=port)
# demo.launch()

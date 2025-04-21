import gradio
import scraper
import os
import zipfile
import time
from csv_files import csv_files

def zip_outputs():
    zip_path = "outputs.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk("outputs"):
            for file in files:
                filepath = os.path.join(root, file)
                arcname = os.path.relpath(filepath, "outputs")
                zipf.write(filepath, arcname=arcname)
    time.sleep(1)
    return zip_path

def run_stream_and_download():
    for log, progress, scraped in scraper.run_scraper_stream():
        file = zip_outputs() if progress >= 1.0 else None

        # Format the file list with ✅
        updated_list = ""
        for fname in csv_files:
            if fname in scraped:
                updated_list += f"✅ {fname}\n"
            else:
                updated_list += f"▫️ {fname}\n"

        yield log, progress, file, updated_list.strip()


with gr.Blocks(title="BigBasket Web Scraper") as demo:
    gr.Markdown("# 🛒 BigBasket Web Scraper GUI")
    gr.Markdown("Click **Start Scraping** to begin. Logs will appear below, and progress will update in real-time.\n\nA download link appears when it's done.")

    with gr.Row():
        with gr.Column(scale=2):
            start_btn = gr.Button("Start Scraping")
            logbox = gr.Textbox(label="Live Logs", lines=25, interactive=False)
        with gr.Column(scale=2):
            file_status = gr.Textbox(label="Scraped CSVs", lines=10, interactive=False)
            download = gr.File(label="📥 Download Results (Auto)", visible=True)
            progress = gr.Slider(minimum=0, maximum=1, step=0.01, label="Progress", interactive=False)


    start_btn.click(fn=run_stream_and_download, outputs=[logbox, progress, download, file_status])

demo.launch()

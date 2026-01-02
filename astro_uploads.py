import os
import datetime
from flask import Blueprint, request, jsonify, send_file, current_app
from werkzeug.utils import secure_filename
import pytz

# Blueprint
bp = Blueprint("astro_uploads", __name__)

# Folder for storing uploaded CSVs
UPLOAD_FOLDER = "astro_uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global active CSV path
CURRENT_ASTRO_PATH = None

# Allowed extensions
ALLOWED_EXT = {"csv"}


def allowed(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def get_csv_collection():
    return current_app.config["CSV_UPLOADS_COLLECTION"]


# ==========================================================
#  UPLOAD CSV (Manual activation only)
# ==========================================================
@bp.route("/upload_csv", methods=["POST"])
def upload_csv():
    global CURRENT_ASTRO_PATH
    csv_uploads_collection = get_csv_collection()

    if "file" not in request.files:
        return jsonify({"success": False, "message": "No file part"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"success": False, "message": "No file selected"}), 400

    if not allowed(file.filename):
        return jsonify({"success": False, "message": "Only CSV files allowed"}), 400

    filename = secure_filename(file.filename)
    save_path = os.path.join(UPLOAD_FOLDER, filename)

    # Save file
    file.save(save_path)

    # Remove old entries for same filename
    csv_uploads_collection.delete_many({"filename": filename})

    # 1. Define the IST timezone object using pytz
    ist_timezone = pytz.timezone('Asia/Kolkata')

    # Insert new CSV (inactive)                                                
    csv_uploads_collection.insert_one({
        "filename": filename,
        "path": save_path,
        "uploaded_at": datetime.datetime.now(ist_timezone).isoformat(),
        "valid": True,
        "active": False
    })
# Insert new CSV (inactive)
# csv_uploads_collection.insert_one({
#     "filename": filename,
#     "path": save_path,
#     # 2. Get the current time and make it aware of the IST timezone
#     "uploaded_at": datetime.datetime.now(ist_timezone).isoformat(),
#     "valid": True,
#     "active": False
# })
    print(f"[UPLOAD] Stored CSV (inactive): {filename}")

    return jsonify({
        "success": True,
        "message": "CSV uploaded successfully (activate manually)",
        "filename": filename,
        "path": save_path
    })


# ==========================================================
#  LIST CSVs
# ==========================================================
@bp.route("/astro_uploads", methods=["GET"])
def list_uploads():
    global CURRENT_ASTRO_PATH
    csv_uploads_collection = get_csv_collection()

    rows = list(csv_uploads_collection.find({}, {"_id": 0}).sort("uploaded_at", -1))

    active_file = None

    # Determine active file
    active_doc = csv_uploads_collection.find_one({"active": True})
    if active_doc:
        active_file = active_doc["filename"]
        CURRENT_ASTRO_PATH = active_doc["path"]
    else:
        CURRENT_ASTRO_PATH = None

    return jsonify({
        "success": True,
        "active_file": active_file,
        "uploads": rows
    })


# ==========================================================
#  ACTIVATE CSV (Fully Fixed)
# ==========================================================
@bp.route("/astro_activate", methods=["POST"])
def activate_csv():
    global CURRENT_ASTRO_PATH
    csv_uploads_collection = get_csv_collection()
    data = request.json or {}
    filename = data.get("filename")

    if not filename:
        return jsonify({"success": False, "message": "Filename missing"}), 400

    # Get CSV record
    doc = csv_uploads_collection.find_one({"filename": filename})
    if not doc:
        return jsonify({"success": False, "message": "CSV not found in DB"}), 404

    full_path = doc["path"]

    if not os.path.exists(full_path):
        return jsonify({"success": False, "message": "CSV file missing on disk"}), 404

    # Deactivate all others
    csv_uploads_collection.update_many({}, {"$set": {"active": False}})

    # Activate this one
    csv_uploads_collection.update_one(
        {"filename": filename},
        {"$set": {"active": True}}
    )

    CURRENT_ASTRO_PATH = full_path

    print(f"[ACTIVATE] {filename} is now active")

    return jsonify({
        "success": True,
        "message": f"{filename} activated successfully",
        "path": full_path
    })
# ==========================================================
#  DOWNLOAD CSV
# ==========================================================
@bp.route("/astro_download/<filename>", methods=["GET"])
def download_csv(filename):
    full_path = os.path.join(UPLOAD_FOLDER, filename)

    if not os.path.exists(full_path):
        return jsonify({"success": False, "message": "File not found"}), 404

    return send_file(full_path, as_attachment=True)


# ==========================================================
#  DELETE CSV (Fully Fixed)
# ==========================================================
@bp.route("/astro_delete", methods=["POST"])
def delete_csv():
    csv_uploads_collection = get_csv_collection()
    data = request.json or {}
    filename = data.get("filename")

    if not filename:
        return jsonify({"success": False, "message": "Filename missing"}), 400

    # Delete DB record
    csv_uploads_collection.delete_one({"filename": filename})

    # Delete file from disk
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if os.path.exists(file_path):
        os.remove(file_path)

    print(f"[DELETE] CSV deleted: {filename}")

    return jsonify({
        "success": True,
        "message": f"{filename} deleted successfully"
    })

import datetime
 # <-- You need to import this





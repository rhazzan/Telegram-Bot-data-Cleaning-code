from flask import Flask, request, jsonify
import pandas as pd
import io

app = Flask(__name__)

# ---------------------------------------
# STEP 1: EXTRACT DATA FROM MAKE REQUEST
# ---------------------------------------
def extract_data_from_request(req):
    """
    This function retrieves incoming CSV data
    sent from Make.com via HTTP POST request.
    """

    # Option 1: File upload
    if "file" in req.files:
        file = req.files["file"]
        df = pd.read_csv(file)
        return df

    # Option 2: Raw CSV text inside JSON body
    if req.is_json and "csv_data" in req.json:
        csv_text = req.json["csv_data"]
        df = pd.read_csv(io.StringIO(csv_text))
        return df

    # If neither provided
    raise ValueError("No CSV data received from Make.")


# ---------------------------------------
# STEP 2: CLEANING LOGIC
# ---------------------------------------
def clean_dataframe(df):
    """
    Modify this function to customize cleaning logic.
    """

    # Remove duplicates
    df = df.drop_duplicates()

    # Remove fully empty rows
    df = df.dropna(how="all")

    # Clean column names
    df.columns = df.columns.str.strip()

    # Strip whitespace from string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Convert numeric-like columns
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")

    # Remove negative values in numeric columns
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        df = df[df[col] >= 0]

    df = df.reset_index(drop=True)

    return df


# ---------------------------------------
# STEP 3: BUILD RESPONSE BACK TO MAKE
# ---------------------------------------
def build_response(df):
    """
    Converts cleaned DataFrame into JSON response
    that Make will automatically receive.
    """
    return {
        "status": "success",
        "row_count": len(df),
        "columns": list(df.columns),
        "rows": df.to_dict(orient="records")
    }


# ---------------------------------------
# HEALTH CHECK
# ---------------------------------------
@app.route("/", methods=["GET"])
def home():
    return "CSV Cleaning API is running."


# ---------------------------------------
# MAIN CLEANING ENDPOINT
# ---------------------------------------
@app.route("/clean", methods=["POST"])
def clean_csv():

    try:
        # 1️⃣ Receive data from Make
        raw_df = extract_data_from_request(request)

        # 2️⃣ Clean the data
        cleaned_df = clean_dataframe(raw_df)

        # 3️⃣ Return cleaned data back to Make
        response_payload = build_response(cleaned_df)

        return jsonify(response_payload), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 400


# ---------------------------------------
# RUN SERVER
# ---------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

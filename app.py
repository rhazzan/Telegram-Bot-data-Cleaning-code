from flask import Flask, request, send_file, jsonify
import pandas as pd
import io

app = Flask(__name__)

# ======================================================
# UTILITY FUNCTIONS
# ======================================================

def add_percentage_to_amount_table(df, amount_column='Amount'):
    table = df.copy()
    total_amount = table[amount_column].sum()

    if total_amount == 0:
        table['% of Total'] = 0
    else:
        table['% of Total'] = (table[amount_column] / total_amount * 100).round(2)

    return table


def add_percentage_columns(pivot_df,
                           debit_col='Debit(₦)',
                           credit_col='Credit(₦)'):

    df = pivot_df.copy()

    if debit_col not in df.columns:
        df[debit_col] = 0

    if credit_col not in df.columns:
        df[credit_col] = 0

    total_debit = df[debit_col].sum()
    total_credit = df[credit_col].sum()
    total_flow = total_debit + total_credit

    df['% Debit'] = 0 if total_debit == 0 else (df[debit_col] / total_debit * 100).round(2)
    df['% Credit'] = 0 if total_credit == 0 else (df[credit_col] / total_credit * 100).round(2)
    df['% Total Flow'] = 0 if total_flow == 0 else (
        (df[debit_col] + df[credit_col]) / total_flow * 100
    ).round(2)

    return df


# ======================================================
# MAIN PROCESSING ROUTE
# ======================================================

@app.route("/process", methods=["POST"])
def process_file():

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    uploaded_file = request.files["file"]

    try:
        # Read wallet sheet
        df = pd.read_excel(uploaded_file, header=6)

        # =========================
        # CLEAN WALLET DATA
        # =========================

        df['Trans. Date1'] = pd.to_datetime(df['Trans. Date'], errors='coerce')
        df['Trans. Date'] = df['Trans. Date1'].dt.date
        df['Time'] = df['Trans. Date1'].dt.time

        df['Debit(₦)'] = (
            df['Debit(₦)'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('--', '0')
            .astype(float)
        )

        df['Credit(₦)'] = (
            df['Credit(₦)'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('--', '0')
            .astype(float)
        )

        df['Transaction Type'] = df.apply(
            lambda x: 'Debit(₦)' if x['Debit(₦)'] > 0 else 'Credit(₦)',
            axis=1
        )

        df['Amount'] = df.apply(
            lambda x: x['Debit(₦)'] if x['Debit(₦)'] > 0 else x['Credit(₦)'],
            axis=1
        )

                # Convert 'Trans. Date' to datetime and extract Date/Time
        df['Trans. Date1'] = pd.to_datetime(df['Trans. Date'], format='%d %b %Y %H:%M:%S')
        df['Trans. Date'] = df['Trans. Date1'].dt.date
        df['Time'] = df['Trans. Date1'].dt.time

        # 7. Extract only the name from Description
        def extract_name(desc):
            desc = str(desc)
            # Check if 'from' or 'to' exists
            if 'from' in desc.lower():
                name = desc.lower().split('from')[1].split('|')[0].strip()
            elif 'to' in desc.lower():
                name = desc.lower().split('to')[1].split('|')[0].strip()
            else:
                name = desc.split('|')[0].strip()
            # Capitalize first letters
            return name.title()

        df['Transaction Name'] = df['Description'].apply(extract_name)

        # Handle Debit and Credit
        df['Debit(₦)'] = df['Debit(₦)'].replace('--', 0).replace(',', '', regex=True).astype(float)
        df['Credit(₦)'] = df['Credit(₦)'].replace('--', 0).replace(',', '', regex=True).astype(float)

        # Create transaction type and unified amount
        df['Transaction Type'] = df.apply(lambda x: 'Debit(₦)' if x['Debit(₦)'] > 0 else 'Credit(₦)', axis=1)
        df['Amount'] = df.apply(lambda x: x['Debit(₦)'] if x['Debit(₦)'] > 0 else x['Credit(₦)'], axis=1)
        df = df.drop(columns=['Debit(₦)', 'Credit(₦)'])

        # Split Description column
        desc_splits = df['Description'].str.split('|', expand=True)
        desc_columns = ['Transaction To/From', 'Platform', 'Account/Phone', 'Extra Info']
        desc_splits.columns = desc_columns[:desc_splits.shape[1]]

        # Correct swapped Platform and Account/Phone
        def fix_swap(row):
            platform = str(row['Platform']).strip()
            account = str(row['Account/Phone']).strip()
            
            # Check if Platform is mostly digits and Account/Phone is letters (network name)
            if platform.replace(' ', '').isdigit() and any(c.isalpha() for c in account):
                row['Platform'], row['Account/Phone'] = account, platform
            return row

        desc_splits = desc_splits.apply(fix_swap, axis=1)

        # Merge back into dataframe
        df = pd.concat([df, desc_splits], axis=1)
        df = df.drop(columns= "Value Date")

        # 8. Reorder columns
        cols_order = ['Transaction Reference', 'Trans. Date', 'Time', 'Transaction Type','Transaction To/From', 'Transaction Name', 'Account/Phone', 'Platform',   
                    'Channel', 'Extra Info', 'Amount','Balance After(₦)']
        df = df[cols_order]


        # =========================
        # WALLET ANALYSIS
        # =========================

        total_debit = df.loc[df['Transaction Type'] == 'Debit(₦)', 'Amount'].sum()
        total_credit = df.loc[df['Transaction Type'] == 'Credit(₦)', 'Amount'].sum()

        latest_balance = df.sort_values(
            by='Trans. Date'
        ).iloc[-1]['Balance After(₦)']

        wallet_summary = pd.DataFrame({
            "Metric": [
                "Total Debit",
                "Total Credit",
                "Current Balance"
            ],
            "Value": [
                total_debit,
                total_credit,
                latest_balance
            ]
        })

        # ========================================
        # 1️⃣ OVERALL FINANCIAL SUMMARY
        # ========================================

        total_debit = df.loc[df['Transaction Type'] == 'Debit(₦)', 'Amount'].sum()
        total_credit = df.loc[df['Transaction Type'] == 'Credit(₦)', 'Amount'].sum()

        total_debit_count = df[df['Transaction Type'] == 'Debit(₦)'].shape[0]
        total_credit_count = df[df['Transaction Type'] == 'Credit(₦)'].shape[0]
        latest_balance = df.sort_values(
            by='Trans. Date'
        ).iloc[-1]['Balance After(₦)']

        summary_df = pd.DataFrame({
            "Metric": [
                "Total Debit Amount",
                "Total Credit Amount",
                "Number of Debit Transactions",
                "Number of Credit Transactions",
                "Current Balance"
            ],
            "Value (₦)": [
                total_debit,
                total_credit,
                total_debit_count,
                total_credit_count,
                float(latest_balance)
            ]
        })

        # ========================================
        # 2️⃣ MONTHLY CASH FLOW SUMMARY (January Format)
        # ========================================

        df['Month'] = df['Trans. Date'].dt.month_name()

        monthly_summary = df.pivot_table(
            index='Month',
            columns='Transaction Type',
            values='Amount',
            aggfunc='sum',
            fill_value=0
        )
        monthly_summary = add_percentage_columns(monthly_summary)

        # Ensure correct month order
        month_order = [
            "January","February","March","April","May","June",
            "July","August","September","October","November","December"
        ]

        monthly_summary = monthly_summary.reindex(month_order).dropna(how='all')

        # ========================================
        # 3️⃣ PLATFORM PERFORMANCE SUMMARY
        # ========================================

        platform_summary = df.pivot_table(
            index='Platform',
            columns='Transaction Type',
            values='Amount',
            aggfunc='sum',
            fill_value=0
        )
        platform_summary = add_percentage_columns(platform_summary)

        # ========================================
        # 4️⃣ DAILY TRANSACTION TREND (Better Format)
        # ========================================

        df['Date_Only'] = df['Trans. Date'].dt.date

        daily_summary = df.pivot_table(
            index='Date_Only',
            columns='Transaction Type',
            values='Amount',
            aggfunc='sum',
            fill_value=0
        ).sort_index()

        # ========================================
        # 5️⃣ TOP 10 SPENDING RECIPIENTS
        # ========================================

        top_spending = (
            df[df['Transaction Type'] == 'Debit(₦)']
            .groupby('Transaction To/From')['Amount']
            .sum()
            .sort_values(ascending=False)
            # .head(10)
            .reset_index()
        )
        top_spending = add_percentage_to_amount_table(top_spending)

        # ========================================
        # 6️⃣ TOP 10 INCOME SOURCES
        # ========================================

        top_income = (
            df[df['Transaction Type'] == 'Credit(₦)']
            .groupby('Transaction To/From')['Amount']
            .sum()
            .sort_values(ascending=False)
            # .head(10)
            .reset_index()
        )
        top_income = add_percentage_to_amount_table(top_income)

        # =========================
        # SAVINGS PROCESSING
        # =========================

        savings_summary = None


        try:
            uploaded_file.seek(0)
            savings_df = pd.read_excel(
                uploaded_file,
                sheet_name='Savings Account Transactions',
                header=6
            )

            if not savings_df.empty:

                savings_df['Trans. Date'] = pd.to_datetime(
                    savings_df['Trans. Date'],
                    errors='coerce'
                )

                for col in ['Debit(₦)', 'Credit(₦)', 'Balance After(₦)']:
                    savings_df[col] = (
                        savings_df[col].astype(str)
                        .str.replace(',', '', regex=False)
                        .replace('--', '0')
                        .astype(float)
                    )

                interest_df = savings_df[
                    savings_df['Description'].str.contains(
                        'Interest',
                        case=False,
                        na=False
                    )
                ]

                total_interest = interest_df['Credit(₦)'].sum()

                savings_summary = pd.DataFrame({
                    "Metric": ["Total Interest Earned"],
                    "Value": [total_interest]
                })

        except:
            savings_summary = None

        try:
            uploaded_file.seek(0)
            savings_df = pd.read_excel(
                uploaded_file,
                sheet_name='Savings Account Transactions',
                header=6
            )

            # Check if sheet is completely empty
            if savings_df.empty:
                print("Savings sheet exists but has no rows. Skipping savings analysis.")
            else:
                print(f"Savings sheet has {len(savings_df)} rows before cleaning.")

                # Strip column names (very important)
                savings_df.columns = savings_df.columns.str.strip()

                print("Columns found:", savings_df.columns.tolist())

                # Expected columns
                required_columns = [
                    'Trans. Date',
                    'Description',
                    'Debit(₦)',
                    'Credit(₦)',
                    'Balance After(₦)',						
                    'Channel'	,
                    'Transaction Reference'

                ]

                # Validate required columns exist
                missing_cols = [col for col in required_columns if col not in savings_df.columns]
                if missing_cols:
                    print("Missing required columns:", missing_cols)
                    print("Check your column names carefully (case-sensitive).")
                else:

                    # Work on a COPY to avoid SettingWithCopyWarning
                    df_savings = savings_df.copy()

                    # ----------------------------
                    # CLEAN DATE COLUMN
                    # ----------------------------
                    df_savings.loc[:, 'Trans. Date'] = pd.to_datetime(
                        df_savings['Trans. Date'],
                        errors='coerce'
                    )
                    # ----------------------------
                    # CLEAN NUMERIC COLUMNS
                    # ----------------------------
                    numeric_cols = ['Debit(₦)', 'Credit(₦)', 'Balance After(₦)']

                    for col in numeric_cols:
                        df_savings.loc[:, col] = (
                            df_savings[col]
                            .astype(str)
                            .str.replace(',', '', regex=False)
                            .replace('--', '0')
                        )

                        df_savings.loc[:, col] = pd.to_numeric(
                            df_savings[col],
                            errors='coerce'
                        ).fillna(0)

                    # Remove rows where Date is NaT
                    df_savings = df_savings[df_savings['Trans. Date'].notna()]

                    print(f"Rows after cleaning: {len(df_savings)}")

                    if df_savings.empty:
                        print("All rows became invalid after cleaning. No savings sheets will be created.")
                    else:

                        # =========================
                        # TOTAL INTEREST
                        # =========================
                        interest_df = df_savings[
                            df_savings['Description'].str.contains(
                                'Interest',
                                case=False,
                                na=False
                            )
                        ]

                        total_interest = interest_df['Credit(₦)'].sum()

                        summary_df = pd.DataFrame({
                            'Metric': ['Total Interest Earned'],
                            'Value': [total_interest]
                        })

                        print("Total interest calculated:", total_interest)

                        # =========================
                        # SAVINGS BALANCE (Latest)
                        # =========================
                        latest_balance = df_savings.sort_values(
                            by='Trans. Date'
                        ).iloc[-1]['Balance After(₦)']

                        balance_df = pd.DataFrame({
                            'Metric': ['Latest Savings Balance'],
                            'Value': [latest_balance]
                        })

                        # =========================
                        # INTEREST BY SAVINGS TYPE
                        # =========================
                        interest_by_type = (
                            interest_df
                            .groupby('Description')['Credit(₦)']
                            .sum()
                            .reset_index()
                        )

                        # =========================
                        # BALANCE BY SAVINGS TYPE
                        # =========================
                        balance_by_type = (
                            df_savings
                            .groupby('Description')['Balance After(₦)']
                            .max()
                            .reset_index()
                )
        except:
            savings_summary = None
        # =========================
        # CREATE OUTPUT FILE IN MEMORY
        # =========================

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Cleaned_Data", index=False)
            wallet_summary.to_excel(writer, sheet_name="Wallet_Analysis", index=False)

            if savings_summary is not None:
                savings_summary.to_excel(
                    writer,
                    sheet_name="Savings_Analysis",
                    index=False
                )

        output.seek(0)

        return send_file(
            output,
            download_name="processed_statement.xlsx",
            as_attachment=True,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ======================================================
# START SERVER
# ======================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

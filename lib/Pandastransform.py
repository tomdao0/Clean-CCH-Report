import pandas as pd
import re
import os
import openpyxl
from lib.transform import CreateTableInSQLServer


class WIPARRecon:

    def __init__(self, folder_path):
        self.folder_path = folder_path

    def find_date(self, txt):
        dates = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", txt)
        first_date = dates[1] if dates else None
        return first_date

    def clean_column_name(self, col_name):
        return re.sub(r"[^a-zA-Z]", "", col_name)

    def process_files(self):
        # List to store DataFrames
        df_list = []

        # Get list of .xlsx files
        xlsx_files = [
            f
            for f in os.listdir(self.folder_path)
            if f.endswith(".xlsx") and not (f.startswith("~"))
        ]
        # From here to process each file: This code will be determine what position of special cell in this firm
        file_path = os.path.join(self.folder_path, xlsx_files[0])
        df = pd.read_excel(file_path, sheet_name=1)
        df = df.astype(str)

        # Find row contains header
        found = False
        for row in df.index:
            for col_idx in range(df.shape[1]):
                if df.iloc[row, col_idx]:
                    if df.iloc[row, col_idx] == "WIP Beg\nBalance":
                        row_header = row
                        found = True
                        break
            if found:
                break
        # Determine cols name to keep
        keep_cols = []
        for col_idx in range(df.shape[1]):
            if df.iloc[row_header, col_idx] != "nan":
                keep_cols.append(col_idx)
        # Determine CutOffDate
        found = False

        for row in range(16):  # Because this cell always before 16 rows
            for col in df.columns:
                if df.at[row, col] and isinstance(df.at[row, col], str):
                    if df.at[row, col][:28] == "For Accounting period dates:":
                        # CutOffDate = self.find_date(df.at[row, col])
                        row_cutoff, col_cutoff = row, col
                        found = True
                        break  # Break the inner loop
            if found:
                break
        # Process each file
        for file in xlsx_files:
            file_path = os.path.join(self.folder_path, file)
            df = pd.read_excel(file_path, sheet_name=1)
            df["CutOffDate"] = self.find_date(df.at[row_cutoff, col_cutoff])
            df = df.astype(str)
            df = df.iloc[:, keep_cols]
            df.columns = [
                self.clean_column_name(df.iloc[row_header, col])
                for col in range(df.shape[1])
            ]
            df = df.drop(df.index[: row_header + 1]).reset_index(drop=True)
            df["ClientIdSubId"] = df["WIPBegBalance"]
            # Exclude rows has "Grand Total" to the end
            for row in reversed(df.index):
                if df.iloc[row, 0] == "Grand Totals":
                    df.drop(df.index[row:], inplace=True)
                    break
            for row in df.index:
                if len(df.at[row, "ClientIdSubId"].split(" ")) == 6:
                    df.at[row, "ClientIdSubId"] = df.at[row, "ClientIdSubId"].split(
                        " "
                    )[5]
                else:
                    df.at[row, "ClientIdSubId"] = df.at[row - 1, "ClientIdSubId"]
            df = df[df["Hours"] != "nan"]
            df["RealPercent"] = df["RealPercent"].replace("%", "", regex=True)
            df_list.append(df)
        final_df = pd.concat(df_list, ignore_index=True)
        
        for i in final_df.columns:
            if i != "ClientIdSubId":
                final_df[i] = pd.to_numeric(final_df[i], errors="coerce")
        return final_df


class WIPARAging:

    def __init__(self, folder_path):
        self.folder_path = folder_path

    def find_date(self, txt):
        dates = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", txt)
        first_date = dates[1] if dates else None
        return first_date

    def clean_column_name(self, col_name):
        return re.sub(r"[^a-zA-Z0-9]", "", col_name)

    def get_payment(self, txt):
        date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", txt)
        amount_match = re.search(r"\$\d{1,3}(?:,\d{3})*(?:\.\d{2})?", txt)
        LastPaymentDate = date = date_match.group() if date_match else "1/1/1900"
        LastPaymentAmount = (
            amount_match.group().replace("$", "").replace(",", "")
            if amount_match
            else 0
        )
        return LastPaymentDate, LastPaymentAmount

    def process_files(self):
        # List to store DataFrames
        df_list = []

        # Get list of .xlsx files
        xlsx_files = [
            f
            for f in os.listdir(self.folder_path)
            if f.endswith(".xlsx") and not (f.startswith("~"))
        ]
        # From here to process each file: This code will be determine what position of special cell in this firm
        file_path = os.path.join(self.folder_path, xlsx_files[0])
        df = pd.read_excel(file_path, sheet_name=1)
        df = df.astype(str)

        # Find row contains header
        found = False
        for row in df.index:
            for col_idx in range(df.shape[1]):
                if df.iloc[row, col_idx]:
                    if df.iloc[row, col_idx] == "Total":
                        row_header = row
                        found = True
                        break
            if found:
                break
        # List all cols name to keep
        # Keep ClientIdSubId column
        keep_cols = []
        found = False
        for row in range(row_header, df.shape[0]):
            for col_idx in range(df.shape[1]):
                if df.iloc[row, col_idx]:
                    if df.iloc[row, col_idx][:18] == "Client ID Sub ID :":
                        keep_cols.append(col_idx)
                        found = True
                        break
            if found:
                break
        # Keep type of transaction column
        found = False
        for row in range(row_header, df.shape[0]):
            for col_idx in range(df.shape[1]):
                if df.iloc[row, col_idx]:
                    if df.iloc[row, col_idx][:18] == "AR":
                        keep_cols.append(col_idx)
                        found = True
                        break
            if found:
                break
        # Keep all column has title
        for col_idx in range(df.shape[1]):
            if df.iloc[row_header, col_idx] != "nan":
                keep_cols.append(col_idx)

        # Determine CutOffDate
        found = False
        for row in range(16):  # Because this cell always before 16 rows
            for col in df.columns:
                if df.at[row, col] and isinstance(df.at[row, col], str):
                    if df.at[row, col][:20] == "For WIP dates as of:":

                        row_cutoff, col_cutoff = row, col
                        found = True
                        break  # Break the inner loop
            if found:
                break

        # Process each file
        for file in xlsx_files:
            file_path = os.path.join(self.folder_path, file)
            df = pd.read_excel(file_path, sheet_name=1)
            CutOffDate = self.find_date(df.at[row_cutoff, col_cutoff])
            df = df.astype(str)
            df = df.iloc[:, keep_cols]
            df.columns = [
                self.clean_column_name(df.iloc[row_header, col])
                for col in range(df.shape[1])
            ]
            df.columns.values[0] = "LastPaymentDate"
            df.columns.values[1] = "Type"
            df["ClientIdSubId"] = df["LastPaymentDate"]
            df["LastPaymentAmount"] = df["LastPaymentDate"]
            df = df.drop(df.index[: row_header + 1]).reset_index(drop=True)
            # Exclude rows has "Grand Total" to the end
            for row in reversed(df.index):
                if df.iloc[row, 0] == "Grand Totals :":
                    df.drop(df.index[row:], inplace=True)
                    break
            # Find Step:
            Step_For = None
            for row in range(1, df.shape[0]):  # Find from second row
                if df.at[row, "ClientIdSubId"][:3] == "Cli":
                    Step_For = row
                    break
            # Fill down ClientIdSubId
            for row in range(0, df.shape[0], Step_For):
                if df.at[row, "ClientIdSubId"][:3] == "Cli":
                    df.at[row, "ClientIdSubId"] = df.at[row, "ClientIdSubId"].split(
                        " "
                    )[5]
                    for i in range(1, Step_For):
                        df.at[row + i, "ClientIdSubId"] = df.at[row, "ClientIdSubId"]
            # Filter Out Type = nan
            df = df[df["Type"] != "nan"]
            # Fill down LastPaymentDate
            for row in df.index:
                if df.at[row, "LastPaymentDate"][:3] == "Las":
                    df.at[row, "LastPaymentDate"] = self.get_payment(
                        df.at[row, "LastPaymentDate"]
                    )[0]
                    df.at[row + 1, "LastPaymentDate"] = df.at[row, "LastPaymentDate"]
                    df.at[row, "LastPaymentAmount"] = self.get_payment(
                        df.at[row, "LastPaymentAmount"]
                    )[1]
                    df.at[row + 1, "LastPaymentAmount"] = df.at[
                        row, "LastPaymentAmount"
                    ]
            df = df.pivot(index="ClientIdSubId", columns="Type")
            df = df.reset_index()
            df.columns = [
                f"{col[0]}_{col[1]}" if col[1] != "" else col[0] for col in df.columns
            ]
            df.drop(
                columns=["LastPaymentDate_AR", "LastPaymentAmount_AR"], inplace=True
            )
            df.rename(
                columns={
                    "LastPaymentAmount_WIP": "LastPaymentAmount",
                    "LastPaymentDate_WIP": "LastPaymentDate",
                },
                inplace=True,
            )
            df["CutOffDate"] = CutOffDate
            df_list.append(df)
        final_df = pd.concat(df_list, ignore_index=True)
        for i in final_df.columns:
            if i not in [
                "ClientIdSubId",
                "LastPaymentAmount",
                "LastPaymentDate",
                "CutOffDate",
            ]:
                final_df[i] = pd.to_numeric(final_df[i], errors="coerce")
        return final_df

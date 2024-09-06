import pandas as pd
import urllib.parse
import sqlalchemy
from sqlalchemy import create_engine, text
import openpyxl
import re
from datetime import datetime
import pytz
import os


class StaffMonthly:

    def __init__(self, folder_path, utcFormat):
        self.folder_path = folder_path
        self.utcFormat = utcFormat

    def process_files(self):
        # Define timezone and current time based on UTC
        utc_minus = pytz.timezone(self.utcFormat)
        current_time_utc_minus = datetime.now(utc_minus).strftime("%Y-%m-%d %H:%M:%S")

        # List to store DataFrames
        df_list = []

        # Get list of .xlsx files
        xlsx_files = [
            f
            for f in os.listdir(self.folder_path)
            if f.endswith(".xlsx") and not (f.startswith("~"))
        ]

        # Process each file
        for file in xlsx_files:
            file_path = os.path.join(self.folder_path, file)
            wb = openpyxl.load_workbook(file_path)
            sheet = wb.worksheets[1]
            last_row = sheet.max_row
            D = {}
            l = [
                "Production Hours",
                "Production Amounts",
                "Billed Hours",
                "Billed Amounts",
                "Billed Write +/- Amounts",
            ]
            # Find last row with "Grand Totals :"
            for row in range(last_row - 12, last_row + 1):
                cell = sheet[f"B{row}"]
                if cell.value == "Grand Totals":
                    last_row = row - 1
                    break

            # Find first row with "Staff ID"
            for row in range(1, last_row):
                if sheet[f"B{row}"].value is not None:
                    if sheet[f"B{row}"].value[:8] == "Staff ID":
                        first_row = row
                        break

            # Find first row with value starting with "For the Dates"
            for row in range(1, 20):
                if sheet[f"H{row}"].value is not None:
                    if sheet[f"H{row}"].value[:13] == "For the Dates":
                        text = sheet[f"H{row}"].value
                        dates = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", text)
                        first_date = dates[0] if dates else None
                        break

            # Process the data
            for i in range(1, int((last_row - first_row) / 6 + 1)):
                # Because first staff contain year of report so we exclude first record
                if i == 1:
                    StaffID = sheet[f"B{first_row}"].value.split(" ")[3]
                    for j in range(5):
                        Type = l[j]
                        Jan = sheet[f"D{first_row + 2 + j}"].value
                        Feb = sheet[f"E{first_row + 2 + j}"].value
                        Mar = sheet[f"F{first_row + 2 + j}"].value
                        Apr = sheet[f"I{first_row + 2 + j}"].value
                        May = sheet[f"J{first_row + 2 + j}"].value
                        Jun = sheet[f"K{first_row + 2 + j}"].value
                        Jul = sheet[f"L{first_row + 2 + j}"].value
                        Aug = sheet[f"M{first_row + 2 + j}"].value
                        Sep = sheet[f"N{first_row + 2 + j}"].value
                        Oct = sheet[f"Q{first_row + 2 + j}"].value
                        Nov = sheet[f"R{first_row + 2 + j}"].value
                        Dec = sheet[f"S{first_row + 2 + j}"].value
                        Total = sheet[f"T{first_row + 2 + j}"].value
                        D[j + 1] = [
                            StaffID,
                            Type,
                            Jan,
                            Feb,
                            Mar,
                            Apr,
                            May,
                            Jun,
                            Jul,
                            Aug,
                            Sep,
                            Oct,
                            Nov,
                            Dec,
                            Total,
                        ]
                else:
                    StaffID = sheet[f"B{first_row + (i-1) * 6 + 1}"].value.split(" ")[3]
                    for j in range(5):
                        Type = l[j]
                        Jan = sheet[f"D{first_row + (i-1) * 6 + 2 + j}"].value
                        Feb = sheet[f"E{first_row + (i-1) * 6 + 2 + j}"].value
                        Mar = sheet[f"F{first_row + (i-1) * 6 + 2 + j}"].value
                        Apr = sheet[f"I{first_row + (i-1) * 6 + 2 + j}"].value
                        May = sheet[f"J{first_row + (i-1) * 6 + 2 + j}"].value
                        Jun = sheet[f"K{first_row + (i-1) * 6 + 2 + j}"].value
                        Jul = sheet[f"L{first_row + (i-1) * 6 + 2 + j}"].value
                        Aug = sheet[f"M{first_row + (i-1) * 6 + 2 + j}"].value
                        Sep = sheet[f"N{first_row + (i-1) * 6 + 2 + j}"].value
                        Oct = sheet[f"Q{first_row + (i-1) * 6 + 2 + j}"].value
                        Nov = sheet[f"R{first_row + (i-1) * 6 + 2 + j}"].value
                        Dec = sheet[f"S{first_row + (i-1) * 6 + 2 + j}"].value
                        Total = sheet[f"T{first_row + (i-1) * 6 + 2 + j}"].value
                        D[(i - 1) * 6 + j + 1] = [
                            StaffID,
                            Type,
                            Jan,
                            Feb,
                            Mar,
                            Apr,
                            May,
                            Jun,
                            Jul,
                            Aug,
                            Sep,
                            Oct,
                            Nov,
                            Dec,
                            Total,
                        ]
            # Create DataFrame
            df = pd.DataFrame.from_dict(
                D,
                orient="index",
                columns=[
                    "StaffID",
                    "Type",
                    "Jan",
                    "Feb",
                    "Mar",
                    "Apr",
                    "May",
                    "Jun",
                    "Jul",
                    "Aug",
                    "Sep",
                    "Oct",
                    "Nov",
                    "Dec",
                    "Total",
                ],
            )
            df.fillna(0.0, inplace=True)
            df["RunningTime"] = current_time_utc_minus
            df["CutOff"] = first_date

            df_list.append(df)

        final_df = pd.concat(df_list, ignore_index=True)
        return final_df


class WIPARAging:

    def __init__(self, folder_path, utcFormat):
        self.folder_path = folder_path
        self.utcFormat = utcFormat

    def process_files(self):
        # Define timezone and current time based on UTC
        utc_minus = pytz.timezone(self.utcFormat)
        current_time_utc_minus = datetime.now(utc_minus).strftime("%Y-%m-%d %H:%M:%S")

        # List to store DataFrames
        df_list = []

        # Get list of .xlsx files
        xlsx_files = [
            f
            for f in os.listdir(self.folder_path)
            if f.endswith(".xlsx") and not (f.startswith("~"))
        ]

        # Process each file
        for file in xlsx_files:
            file_path = os.path.join(self.folder_path, file)
            wb = openpyxl.load_workbook(file_path)
            sheet = wb.worksheets[1]
            last_row = sheet.max_row
            D = {}

            # Find last row with "Grand Totals :"
            for row in range(last_row - 6, last_row + 1):
                cell = sheet[f"B{row}"]
                if cell.value == "Grand Totals :":
                    last_row = row - 1
                    break

            # Find first row with "Client ID Sub ID"
            for row in range(1, last_row):
                if sheet[f"B{row}"].value is not None:
                    if sheet[f"B{row}"].value[:16] == "Client ID Sub ID":
                        first_row = row
                        break

            # Find first row with value starting with "For WIP dates"
            for row in range(1, 20):
                if sheet[f"G{row}"].value is not None:
                    if sheet[f"G{row}"].value[:13] == "For WIP dates":
                        text = sheet[f"G{row}"].value
                        dates = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", text)
                        first_date = dates[0] if dates else None
                        break

            # Process the data
            for i in range(1, int((last_row - first_row + 1) / 3 + 1)):
                ClientIdSubId = sheet[f"B{(i-1)*3 + first_row}"].value.split(" ")[5]
                ARTotal = sheet[f"D{(i-1)*3 + first_row + 1}"].value
                AR0030 = sheet[f"G{(i-1)*3 + first_row + 1}"].value
                AR3160 = sheet[f"H{(i-1)*3 + first_row + 1}"].value
                AR6190 = sheet[f"I{(i-1)*3 + first_row + 1}"].value
                AR91120 = sheet[f"J{(i-1)*3 + first_row + 1}"].value
                AR121150 = sheet[f"M{(i-1)*3 + first_row + 1}"].value
                AR151180 = sheet[f"N{(i-1)*3 + first_row + 1}"].value
                AROver180 = sheet[f"O{(i-1)*3 + first_row + 1}"].value
                WIPTotal = sheet[f"D{(i-1)*3 + first_row + 2}"].value
                WIP0030 = sheet[f"G{(i-1)*3 + first_row + 2}"].value
                WIP3160 = sheet[f"H{(i-1)*3 + first_row + 2}"].value
                WIP6190 = sheet[f"I{(i-1)*3 + first_row + 2}"].value
                WIP91120 = sheet[f"J{(i-1)*3 + first_row + 2}"].value
                WIP121150 = sheet[f"M{(i-1)*3 + first_row + 2}"].value
                WIP151180 = sheet[f"N{(i-1)*3 + first_row + 2}"].value
                WIPOver180 = sheet[f"O{(i-1)*3 + first_row + 2}"].value
                D[i] = [
                    ClientIdSubId,
                    ARTotal,
                    AR0030,
                    AR3160,
                    AR6190,
                    AR91120,
                    AR121150,
                    AR151180,
                    AROver180,
                    WIPTotal,
                    WIP0030,
                    WIP3160,
                    WIP6190,
                    WIP91120,
                    WIP121150,
                    WIP151180,
                    WIPOver180,
                ]

            # Create DataFrame
            df = pd.DataFrame.from_dict(
                D,
                orient="index",
                columns=[
                    "ClientIdSubId",
                    "ARTotal",
                    "AR0030",
                    "AR3160",
                    "AR6190",
                    "AR91120",
                    "AR121150",
                    "AR151180",
                    "AROver180",
                    "WIPTotal",
                    "WIP0030",
                    "WIP3160",
                    "WIP6190",
                    "WIP91120",
                    "WIP121150",
                    "WIP151180",
                    "WIPOver180",
                ],
            )
            df.fillna(0.0, inplace=True)
            df["RunningTime"] = current_time_utc_minus
            df["CutOff"] = first_date

            df_list.append(df)

        final_df = pd.concat(df_list, ignore_index=True)
        return final_df


class WIPARRecon:

    def __init__(self, folder_path, utcFormat):
        self.folder_path = folder_path
        self.utcFormat = utcFormat

    def process_files(self):
        # Define timezone and current time based on UTC
        utc_minus = pytz.timezone(self.utcFormat)
        current_time_utc_minus = datetime.now(utc_minus).strftime("%Y-%m-%d %H:%M:%S")
        # List to store DataFrames
        df_list = []

        # Get list of .xlsx files
        xlsx_files = [
            f
            for f in os.listdir(self.folder_path)
            if f.endswith(".xlsx") and not (f.startswith("~"))
        ]

        # Process each file
        for file in xlsx_files:
            file_path = os.path.join(self.folder_path, file)
            wb = openpyxl.load_workbook(file_path)
            sheet = wb.worksheets[1]
            last_row = sheet.max_row
            D = {}

            # Find last row with "Grand Totals"
            for row in range(last_row - 6, last_row + 1):
                cell = sheet[f"A{row}"]
                if cell.value == "Grand Totals":
                    last_row = row - 1
                    break

            # Find first row with "Client ID Sub ID"
            for row in range(1, last_row):
                if sheet[f"A{row}"].value is not None:
                    if sheet[f"A{row}"].value[:16] == "Client ID Sub ID":
                        first_row = row
                        break

            # Find first row with value starting with "For Accounting period"
            for row in range(1, 20):
                if sheet[f"I{row}"].value is not None:
                    if sheet[f"I{row}"].value[:21] == "For Accounting period":
                        text = sheet[f"I{row}"].value
                        dates = re.findall(
                            r"\d{1,2}/\d{1,2}/\d{4} - \d{1,2}/\d{1,2}/\d{4}", text
                        )
                        first_date = dates[0].split(" - ")[1] if dates else None
                        break

            # Process the data
            for i in range(1, int((last_row - first_row + 1) / 2 + 1)):
                ClientIdSubId = sheet[f"A{(i-1)*2 + first_row}"].value.split(" ")[5]
                WIPBegin = sheet[f"A{(i-1)*2 + first_row + 1}"].value
                Hours = sheet[f"C{(i-1)*2 + first_row + 1}"].value
                Time = sheet[f"D{(i-1)*2 + first_row + 1}"].value
                Expense = sheet[f"E{(i-1)*2 + first_row + 1}"].value
                Billings = sheet[f"F{(i-1)*2 + first_row + 1}"].value
                WriteUD = sheet[f"G{(i-1)*2 + first_row + 1}"].value
                RealPer = sheet[f"K{(i-1)*2 + first_row + 1}"].value[:-1]
                WIPEnd = sheet[f"L{(i-1)*2 + first_row + 1}"].value
                ARBegin = sheet[f"M{(i-1)*2 + first_row + 1}"].value
                InvoiceSalesTax = sheet[f"N{(i-1)*2 + first_row + 1}"].value
                Adjustment = sheet[f"Q{(i-1)*2 + first_row + 1}"].value
                Charges = sheet[f"R{(i-1)*2 + first_row + 1}"].value
                Payments = sheet[f"S{(i-1)*2 + first_row + 1}"].value
                AREnd = sheet[f"T{(i-1)*2 + first_row + 1}"].value
                D[i] = [
                    ClientIdSubId,
                    WIPBegin,
                    Hours,
                    Time,
                    Expense,
                    Billings,
                    WriteUD,
                    RealPer,
                    WIPEnd,
                    ARBegin,
                    InvoiceSalesTax,
                    Adjustment,
                    Charges,
                    Payments,
                    AREnd,
                ]

            # Create DataFrame
            df = pd.DataFrame.from_dict(
                D,
                orient="index",
                columns=[
                    "ClientIdSubId",
                    "WIPBegin",
                    "Hours",
                    "Time",
                    "Expense",
                    "Billings",
                    "WriteUD",
                    "RealPer",
                    "WIPEnd",
                    "ARBegin",
                    "InvoiceSalesTax",
                    "Adjustment",
                    "Charges",
                    "Payments",
                    "AREnd",
                ],
            )
            df.fillna(0.0, inplace=True)
            df["RunningTime"] = current_time_utc_minus
            df["CutOff"] = first_date

            df_list.append(df)

        final_df = pd.concat(df_list, ignore_index=True)
        return final_df


class CreateTableInSQLServer:
    def __init__(self, SQLServerName, DBName, TableName, UserName, PWD, df_data):
        self.connect_string = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "Server=" + SQLServerName + ";"
            "Database=" + DBName + ";"
            "UID=" + UserName + ";"
            "PWD=" + PWD + ";"
        )
        self.TableName = TableName
        self.df_data = df_data

    def run(self):
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.connect_string}",
            fast_executemany=True,
        )
        with engine.connect() as connection:
            self.df_data.to_sql(
                self.TableName, connection, index=False, if_exists="replace"
            )
            print("OK")

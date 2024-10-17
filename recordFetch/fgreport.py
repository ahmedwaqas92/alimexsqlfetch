import sys
sys.path.append("../")
from libraries.libraries import *
from connection.connection import dbConnection


class fgReportFetch():
    def __init__(self):
        pass
    
    def fetchingTables():
        try:
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = cursor.fetchall()
            for table in tables:
                print(table.TABLE_NAME)
        except pyodbc.Error as err:
            print(f"Error: {err}")
        finally:
            connection.close()
    
    def heatLotCost():
        try:
            heatlot_costDictionary = []
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("select * from GRDTL")
            GRDTLtable = cursor.fetchall()
            
            GRDTLcolumns = [column[0] for column in cursor.description]
            LotNo_Sums = {}
            
            for row in GRDTLtable:
                GRDTL_withColumns = dict(zip(GRDTLcolumns, row))
                
                keys_to_check = ["UDF_LotNo", "LocalTaxableAmt", "UDF_Weight"]
                if all(key in GRDTL_withColumns for key in keys_to_check):
                    lot_no = GRDTL_withColumns["UDF_LotNo"]
                    local_taxable_amt = GRDTL_withColumns["LocalTaxableAmt"]
                    udf_weight = GRDTL_withColumns["UDF_Weight"]
                    
                    if local_taxable_amt is not None and udf_weight is not None:
                        if lot_no not in LotNo_Sums:
                            LotNo_Sums[lot_no] = {"LocalTaxableAmt": 0, "UDF_Weight": 0}

                        LotNo_Sums[lot_no]["LocalTaxableAmt"] += local_taxable_amt
                        LotNo_Sums[lot_no]["UDF_Weight"] += udf_weight
            
            for lot_no, sums in LotNo_Sums.items():
                if sums["UDF_Weight"] != 0:
                    cost_per_weight = round(sums["LocalTaxableAmt"] / sums["UDF_Weight"], 3)
                else:
                    cost_per_weight = 0  # Avoid division by zero

                heatlot_costDictionary.append({
                    "UDF_LotNo": lot_no,
                    "CostPerWeight": cost_per_weight
                })
                
            return heatlot_costDictionary
            
        except pyodbc.Error as err:
            print(f"Error: {err}")
        finally:
            connection.close()

    def generatingFGReport():
        try:
            grnData = fgReportFetch.heatLotCost()
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("select * from IV") 
            invoice_columns = []
            
            for column in cursor.description:
                invoice_columns.append(column[0])
            
            IVtable = cursor.fetchall()
            start_month = 10
            end_month = 10
            report = []
            
            for row in IVtable:
                invoiceWithColumns = dict(zip(invoice_columns, row))
                if invoiceWithColumns and invoiceWithColumns["DocDate"].year == datetime.now().year:
                    invoice_month = invoiceWithColumns["DocDate"].month
                    if start_month <= invoice_month <= end_month:
                        cursor.execute("select * from IVDTL where DocKey = ?", invoiceWithColumns["DocKey"])
                        IVDTLtable = cursor.fetchall()
                        for item in IVDTLtable:
                            invoice_detail_columns = []
                            
                            for column in cursor.description:
                                invoice_detail_columns.append(column[0])
                            
                            itemWithColumnDetails = dict(zip(invoice_detail_columns, item))
                            
                             # Extract required data
                            item_code = itemWithColumnDetails["ItemCode"]
                            doc_date = invoiceWithColumns["DocDate"]
                            item_weight = itemWithColumnDetails["UDF_Weight"]
                            
                            
                            if item_weight is not None:
                                sales_price = itemWithColumnDetails["LocalTaxableAmt"]
                                udf_lot_no = itemWithColumnDetails["UDF_LotNo"]
                                
                                for grn_item in grnData:
                                    if grn_item["UDF_LotNo"] == udf_lot_no:
                                        cost_per_weight = grn_item["CostPerWeight"]
                                        purchase_price = cost_per_weight * item_weight
                                        if sales_price != 0:
                                            margin_percentage = ((sales_price - purchase_price) / sales_price) * 100
                                        else:
                                            margin_percentage = 0
                                            
                                        report.append({
                                            "ItemCode": item_code,
                                            "HeatLotNo": udf_lot_no,
                                            "DocDate": doc_date,
                                            "ItemWeight": item_weight,
                                            "SalesPrice": sales_price,
                                            "PurchasePrice": purchase_price,
                                            "MarginPercentage": round(margin_percentage, 3)  # Round to 3 decimal places
                                        })
                
                                        break  # Exit the loop once the matching UDF_LotNo is found
                                    else:
                                        pass
                                
            
            return report
        
        except pyodbc.Error as err:
            print(f"Error: {err}")

        finally:
            connection.close()
            
    def consolidation():
        report = fgReportFetch.generatingFGReport()
        # Dictionary to hold the consolidated data
        consolidated_data = defaultdict(lambda: {
            "SalesPriceSum": Decimal('0.0'),
            "PurchasePriceSum": Decimal('0.0'),
            "MarginPercentageSum": Decimal('0.0'),
            "Count": 0
        })
        
        # Process each item in the report
        for item in report:
            month = item["DocDate"].strftime("%b")
            item_code = item["ItemCode"]
            key = (month, item_code)
            
            consolidated_data[key]["SalesPriceSum"] += item["SalesPrice"]
            consolidated_data[key]["PurchasePriceSum"] += item["PurchasePrice"]
            consolidated_data[key]["MarginPercentageSum"] += item["MarginPercentage"]
            consolidated_data[key]["Count"] += 1
        
        # Generate the final consolidated report
        final_report = []
        for (month, item_code), data in consolidated_data.items():
            sales_price_sum = data["SalesPriceSum"]
            purchase_price_sum = data["PurchasePriceSum"]
            margin_percentage = (sales_price_sum - purchase_price_sum) / sales_price_sum * 100 if sales_price_sum != 0 else 0
            
            final_report.append({
                "Month": month,
                "ItemCode": item_code,
                "SalesPriceSum": sales_price_sum,
                "PurchasePriceSum": purchase_price_sum,
                "MarginPercentage": round(margin_percentage, 2)
            })
        
        return final_report
    
    def create_pdf(image_path, pdf_path):
        c = canvas.Canvas(pdf_path, pagesize=letter)
        width, height = letter

        # Add the chart image to the PDF
        c.drawImage(image_path, 50, height - 500, width=500, height=400)
        c.showPage()
        c.save()
   
    def report_generation():
        result = fgReportFetch.consolidation()
        df = pd.DataFrame(result)
        # Group by ItemCode and sum the SalesPriceSum and PurchasePriceSum
        grouped_df = df.groupby('ItemCode').agg({
            'SalesPriceSum': 'sum',
            'PurchasePriceSum': 'sum',
        }).reset_index()
        
        grouped_df = grouped_df.loc[grouped_df['SalesPriceSum'] != 0]
        grouped_df['MarginPercentage'] = (grouped_df['SalesPriceSum'] - grouped_df['PurchasePriceSum']) / grouped_df['SalesPriceSum'] * 100
      
        # Generate the bar chart
        fig, ax = plt.subplots(figsize=(20, 12))
        
        bar_width = 0.55
        gap = 0.1
        index = range(len(grouped_df))
        
        # Calculate new x positions with gaps
        sales_x = [i * (2 * bar_width + gap) for i in index]
        purchase_x = [i + bar_width for i in sales_x]
        
        # Plot Sales Price Sum
        sales_bars = ax.bar(sales_x, grouped_df['SalesPriceSum'], bar_width, label='Sales Price Sum')
        
        # Plot Purchase Price Sum
        purchase_bars = ax.bar(purchase_x, grouped_df['PurchasePriceSum'], bar_width, label='Purchase Price Sum')
        
        ax.set_xlabel('Item Code')
        ax.set_ylabel('Amount')
        ax.set_title('FG Report for Oct 2024')
        ax.legend()
        
        ax.set_xticks([i + bar_width / 2 for i in sales_x])
        ax.set_xticklabels(grouped_df['ItemCode'], color='black')
        
        # Add value labels on top of each sales bar
        for bar in sales_bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'{height:.2f}', ha='center', va='bottom', color='black')

        # Add value labels inside each purchase bar
        for bar in purchase_bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2.0, height / 2, f'{height:.2f}', ha='center', va='center', color='black')
            
        # margin_heights = grouped_df['SalesPriceSum'] - grouped_df['PurchasePriceSum']
        
        # Plot Margin Percentage on top of Purchase Price Sum
        margin_bars = ax.bar(purchase_x, grouped_df['MarginPercentage'], bar_width, bottom=grouped_df['PurchasePriceSum'], label='Margin Percentage', color='yellow')
        
        # Add value labels on top of each margin bar
        for bar in margin_bars:
            height = bar.get_height() + bar.get_y()
            item_code = grouped_df.iloc[int(bar.get_x() // (2 * bar_width + gap))]['ItemCode']
            purchase_price_sum = grouped_df.loc[grouped_df['ItemCode'] == item_code, 'PurchasePriceSum'].values[0]
            sales_price_sum = grouped_df.loc[grouped_df['ItemCode'] == item_code, 'SalesPriceSum'].values[0]
            margin_percentage = (sales_price_sum - purchase_price_sum) / sales_price_sum * 100
            ax.text(bar.get_x() + bar.get_width() / 2.0, height, f'{margin_percentage:.2f}%', ha='center', va='bottom', color='black')
        
        # Save the chart as an image
        chart_image_path = 'bar_chartOct.png'
        plt.savefig(chart_image_path)
        plt.close()
        
        # print(f"Bar chart saved as {chart_image_path}")
        
        # Create the PDF
        pdf_path = 'reportOct.pdf'
        fgReportFetch.create_pdf(chart_image_path, pdf_path)
        
        print(f"PDF report saved as {pdf_path}")
        
fgReportFetch.report_generation()
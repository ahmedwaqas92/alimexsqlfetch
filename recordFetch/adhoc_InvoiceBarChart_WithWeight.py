import sys
sys.path.append("../")
from libraries.libraries import *
from connection.connection import dbConnection


class salesOrderFetch:
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
    def salesOrder():
        try:
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("select * from IV") 
            columns = []
            
            for column in cursor.description:
                columns.append(column[0])
            
            table = cursor.fetchall()
            
            total_alimex_sales = 0
            monthly_data = []
            
            for row in table:
                row_dict = dict(zip(columns, row))
                for month in range(1,13):
                    if row_dict.get('DocDate') and row_dict.get('DocDate').year == datetime.now().year and row_dict.get('DocDate').month == month:
                        total_alimex_sales += row_dict.get("LocalTaxableAmt", 0)
                        if row_dict.get("DebtorName") in ["FOXSEMICON INTEGRATED TECHNOLOGY (SHANGHAI) INC."]:
                            cursor.execute("select * from IVDTL where DocKey = ?", row_dict["DocKey"])
                            
                            invoice_details_columns = []
                            
                            for column in cursor.description:
                                invoice_details_columns.append(column[0])
                                
                            record = cursor.fetchall()
                            
                            for data in record:
                                # print(data)
                                itemListing = dict(zip(invoice_details_columns, data))
                                # print(itemListing)
                                if itemListing["ItemCode"] is None or itemListing['FromDocNo'] is None:
                                    pass
                                else:
                                    if "DO" in itemListing["FromDocNo"]:
                                        records = {
                                            "DocNo" : row_dict["DocNo"],
                                            "Date" : row_dict["DocDate"],
                                            "Item Code" : itemListing["ItemCode"],
                                            "Weight" : itemListing["UDF_ActualWeight"],
                                            "Transferred From": itemListing["FromDocNo"],
                                            "Amount EUR" : itemListing["SubTotalExTax"] 
                                        }
                                        monthly_data.append(records)
                                
            return monthly_data, total_alimex_sales
                               
        except pyodbc.Error as err:
            print(f"Error: {err}")

        finally:
            connection.close()
            
    def generatingGraph():
            values, total_alimex_sales= salesOrderFetch.salesOrder()
            
            monthly_data = {}
            monthly_weights = {}
            total_subtotals = {}

            for entry in values:
                item_code = entry['Item Code']
                date = entry['Date']
                weight = entry['Weight']
                subtotal = entry['Amount EUR']
                
                # Extract year and month
                year_month = (date.year, date.month)
                
                if item_code not in monthly_data:
                    monthly_data[item_code] = {}
                
                if year_month not in monthly_data[item_code]:
                    monthly_data[item_code][year_month] = 0
                
                monthly_data[item_code][year_month] += subtotal
        
                # Calculate total subtotal for each item code
                if item_code not in total_subtotals:
                    total_subtotals[item_code] = Decimal('0')
                total_subtotals[item_code] += subtotal
                
                if item_code not in monthly_weights:
                    monthly_weights[item_code] = {}

                if year_month not in monthly_weights[item_code]:
                    monthly_weights[item_code][year_month] = Decimal('0')

                monthly_weights[item_code][year_month] += weight

            # Prepare data for plotting
            months = [datetime(2024, month, 1).strftime('%b') for month in range(1, 13)]
            item_codes = list(monthly_data.keys())
            data = {item_code: [monthly_data[item_code].get((2024, month), 0) for month in range(1, 13)] for item_code in item_codes}
            weight_data = {item_code: [monthly_weights[item_code].get((2024, month), 0) for month in range(1, 13)] for item_code in item_codes}

            # Plotting
            fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(40, 16), gridspec_kw={'width_ratios': [3, 1, 1]})

            bar_width = 0.7  # Width of each bar
            gap = 1.4  # Gap between sets of items
            month_indices = range(len(months))  # Indices for the months
            
            current_datetime = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
            
            total_sales_per_month = [sum(subtotals) for subtotals in zip(*data.values())]
            total_sales = sum(total_sales_per_month)
            
            filtered_months = [month for month, total in zip(months, total_sales_per_month) if total > 0]
            filtered_sales = [total for total in total_sales_per_month if total > 0]

             # Adjust the position of each bar for each item code
            for i, (item_code, subtotals) in enumerate(data.items()):
                bar_positions = [index + (i * bar_width) + (index * gap) for index in month_indices]
                bars = ax1.bar(bar_positions, subtotals, bar_width, label=item_code)
                
                # Add data labels
                for j, bar in enumerate(bars):
                    height = bar.get_height()
                    if height > 0:  # Only add labels for non-zero values
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2.0, 
                            height, 
                            f'{height:,.2f} EUR',  # Add EUR as a string
                            ha='center', 
                            va='bottom',
                            fontsize=8,  # Reduce font size
                            fontdict={'color': 'blue', 'weight': 'bold'}
                        )
                        weight = weight_data[item_code][j]  # Get the weight for the corresponding bar
                        if weight >= 50:  # Only show weight if it is 50KG or more
                            ax1.text(
                                bar.get_x() + bar.get_width() / 2.0, 
                                height / 2,  # Position it inside the bar
                                f'{weight:,.2f} KG', 
                                ha='center', 
                                va='center',
                                fontsize=8,  # Reduce font size
                                fontweight='bold',  # Make the weight bold
                            )

            # Set the x-axis labels to the middle of the grouped bars and shift them slightly to the left
            shift = 0.5  # Adjust this value to shift the labels
            ax1.set_xticks([index + (len(item_codes) * bar_width / 2) + (index * gap) - shift for index in month_indices])
            ax1.set_xticklabels(months)

            ax1.set_xlabel('Month')
            ax1.set_ylabel('Total Sales (EUR)')
            ax1.set_title('Total Sales (EUR) for Each Item by Month for 2024', fontweight='bold')
            ax1.legend(title='Item Code')
            
            plt.figtext(0.5, 0.85, f'Generated on: {current_datetime}', ha='center', fontsize=10, fontweight='bold')  # Added line
        

            # Custom function to display subtotal in currency and percentage
            def autopct_format(values):
                def my_format(pct):
                    total = float(sum(values))  # Convert total to float
                    val = int(round(pct * total / 100.0))
                    return f'{pct:.1f}%\n({val} EUR)'
                return my_format
            
            def autopct_format_with_eur(values):
                def my_format(pct):
                    total = float(sum(values))  # Convert total to float
                    val = int(round(pct * total / 100.0))
                    return f'{pct:.1f}%\n({val:,} EUR)'
                return my_format

            # Pie chart for total subtotal sold based on item code
            ax2.pie([float(v) for v in total_subtotals.values()], labels=total_subtotals.keys(), autopct=autopct_format([float(v) for v in total_subtotals.values()]), startangle=140)
            ax2.set_title('Total Sales (EUR) by Item Code', fontweight='bold')
            
            # fig2, ax3 = plt.subplots(figsize=(8, 8))
            ax3.text(0, 1.15, f'Total Sales for VDL: {total_sales:,.2f} EUR', ha='center', fontsize=10, fontweight='bold')
            colors = plt.cm.Paired(range(len(filtered_months)))  # Use different colors
            ax3.pie(filtered_sales, labels=filtered_months, autopct=autopct_format_with_eur(filtered_sales), startangle=140, colors=colors)
            ax3.set_title('Percentage of Monthly Sales', fontweight='bold')
            
            plt.figtext(0.85, 0.05, f'Total Alimex Sales: {total_alimex_sales:,} MYR', ha='right', fontsize=20, fontweight='bold')  # Added line
            
            plt.subplots_adjust(hspace=0.5)
            plt.tight_layout()
            plt.show()


# salesOrderFetch.fetchingTables()
salesOrderFetch.generatingGraph()
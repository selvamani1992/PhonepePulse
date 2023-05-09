import os
import psycopg2
import json
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px

# Database connectivity segment
selva = psycopg2.connect(host="localhost", user="username", password="password", port=5432, database="Phonepe")
guvi = selva.cursor()


def create_table():
    # database table creation for Aggregated vs Transaction
    guvi.execute("CREATE TABLE Agg_Trans(State varchar(50),"
                 "Year int,"
                 "Quarter varchar(5),"
                 "Data_From bigint,"
                 "Data_To bigint,"
                 "Transaction_Type varchar(50),"
                 "Transaction_Count  int,"
                 "Transaction_Amount real)")
    # selva.commit() # commented - we can create only one table in the same name and to avoid error

    # database table creation for Aggregated vs User
    guvi.execute("CREATE TABLE Agg_User(State varchar(50),"
                 "Year int,"
                 "Quarter varchar(5),"
                 "User_Brand varchar(15),"
                 "User_Count int,"
                 "User_Percentage real)")
    selva.commit()

    # database table creation for Map vs Transaction
    guvi.execute("CREATE TABLE Map_Trans(State varchar(50),"
                 "Year int,"
                 "Quarter varchar(5),"
                 "District varchar(50),"
                 "Total_Count int,"
                 "Total_Amount real)")
    selva.commit()


    # database table creation for Map vs User
    guvi.execute("CREATE TABLE Map_Users(State varchar(50),"
                 "Year int,"
                 "Quarter varchar(5),"
                 "District varchar(50),"
                 "Total_Count int,"
                 "App_opened int)")
    selva.commit()
    # database table for complete indian pincode information
    guvi.execute("CREATE TABLE PINCODE(Circle_Name varchar(50),"
                 "Region_Name varchar(50),"
                 "Division varchar(50),"
                 "Office varchar(50),"
                 "Pincode int,"
                 "Office_type varchar(50),"
                 "Delivery varchar(50),"
                 "District varchar(50),"
                 "State varchar(50))")
    selva.commit()

    # table creation for top_tansaction
    guvi.execute("CREATE TABLE Top_Trans(State varchar(50),"
                 "Year int,"
                 "Quarter varchar(5),"
                 "Pincode int,"
                 "District varchar(50),"
                 "Total_Count int,"
                 "Total_Amount real)")
    selva.commit()

    # table creation for top_user
    guvi.execute("CREATE TABLE Top_users(State varchar(50),"
                 "Year int,"
                 "Quarter varchar(5),"
                 "Pincode int,"
                 "District varchar(50),"
                 "Total_Count int)")
    selva.commit()

    # creating a database to store lat and long of state
    guvi.execute("CREATE TABLE State_Geo(State varchar(50),"
                 "Latitude real,"
                 "Longitude real)")
    selva.commit()

    # creating a database to store lat and long of state


# Indian pincode list from gov site for reference
# https://data.gov.in/catalog/all-india-pincode-directory?filters%5Bfield_catalog_reference%5D=85840&format=json&offset=0&limit=6&sort%5Bcreated%5D=desc
def pincode():
    df_pin = pd.read_csv("D:\Python\PycharmProjects\pythonProject\Phonepe Pulse\Pincode_30052019.csv",
                         encoding='iso-8859-1')
    for row in df_pin.itertuples(index=False):
        guvi.execute("INSERT INTO PINCODE VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
    selva.commit()


def aggregated_transaction(aggr_trans):
    guvi.execute("DELETE FROM Agg_Trans")
    selva.commit()
    states = os.listdir(aggr_trans)
    for state_list in states:
        year = os.listdir(f"{aggr_trans}\{state_list}")
        for year_list in year:
            quar = os.listdir(f"{aggr_trans}\{state_list}\{year_list}")
            for quar_list in quar:

                # print(f"{aggr_trans}\{state_list}\{year_list}\{quar_list}") --> Verification line
                file = f"{aggr_trans}\{state_list}\{year_list}\{quar_list}"
                dt = open(file, 'r')
                d = json.load(dt)
                quarter = "Q"+quar_list.strip('.json')
                sub_range = len(d["data"]['transactionData'])
                for i in range(sub_range):
                    insert_one = "INSERT INTO Agg_Trans VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    guvi.execute(insert_one, (state_list, year_list, quarter, d["data"]["from"], d["data"]["to"],
                                              d["data"]['transactionData'][i]["name"],
                                              d["data"]['transactionData'][i]["paymentInstruments"][0]["count"],
                                              d["data"]['transactionData'][i]["paymentInstruments"][0]["amount"]))
                    selva.commit()


def aggregated_user(aggr_user):
    guvi.execute("DELETE FROM Agg_User")
    selva.commit()
    states = os.listdir(aggr_user)
    for state_list in states:
        year = os.listdir(f"{aggr_user}\{state_list}")
        for year_list in year:
            quar = os.listdir(f"{aggr_user}\{state_list}\{year_list}")
            for quar_list in quar:
                # print(f"{aggr_user}\{state_list}\{year_list}\{quar_list}") --> Verification line
                file = f"{aggr_user}\{state_list}\{year_list}\{quar_list}"
                dt = open(file, 'r')
                d = json.load(dt)
                quarter = "Q"+quar_list.strip('.json')
                if d["data"]["usersByDevice"] is None:
                    insert_one = "INSERT INTO Agg_User VALUES (%s, %s, %s, %s, %s, %s)"
                    guvi.execute(insert_one, (state_list, year_list, quarter,
                                              None,
                                              None,
                                              None))
                    selva.commit()
                else:
                    sub_range = len(d["data"]["usersByDevice"])
                    # print(d)  --> Verification line
                    for i in range(sub_range):
                        insert_one = "INSERT INTO Agg_User VALUES (%s, %s, %s, %s, %s, %s)"
                        guvi.execute(insert_one, (state_list, year_list, quarter,
                                                  d["data"]['usersByDevice'][i]["brand"],
                                                  d["data"]['usersByDevice'][i]["count"],
                                                  d["data"]['usersByDevice'][i]["percentage"]))
                        selva.commit()


def map_transaction(map_trans):
    guvi.execute("DELETE FROM Map_Trans")
    selva.commit()
    states = os.listdir(map_trans)
    for state_list in states:
        year = os.listdir(f"{map_trans}\{state_list}")
        for year_list in year:
            quar = os.listdir(f"{map_trans}\{state_list}\{year_list}")
            for quar_list in quar:
                file = f"{map_trans}\{state_list}\{year_list}\{quar_list}"
                dt = open(file, 'r')
                d = json.load(dt)
                quarter = "Q"+quar_list.strip('.json')
                sub_range = len(d["data"]["hoverDataList"])
                for i in range(sub_range):
                    insert_one = "INSERT INTO Map_Trans VALUES (%s, %s, %s, %s, %s, %s)"
                    guvi.execute(insert_one, (state_list, year_list, quarter,
                                              d["data"]["hoverDataList"][i]["name"],
                                              d["data"]["hoverDataList"][i]["metric"][0]["count"],
                                              d["data"]["hoverDataList"][i]["metric"][0]["amount"]))
                    selva.commit()


def map_users(map_user):
    guvi.execute("DELETE FROM Map_Users")
    selva.commit()
    states = os.listdir(map_user)
    for state_list in states:
        year = os.listdir(f"{map_user}\{state_list}")
        for year_list in year:
            quar = os.listdir(f"{map_user}\{state_list}\{year_list}")
            for quar_list in quar:
                # print(f"{map_trans}\{state_list}\{year_list}\{quar_list}")  --> Verification line
                file = f"{map_user}\{state_list}\{year_list}\{quar_list}"
                dt = open(file, 'r')
                d = json.load(dt)
                quarter = "Q" + quar_list.strip('.json')
                district = list(d['data']['hoverData'].keys())
                for district_list in district:
                    insert_one = "INSERT INTO Map_Users VALUES (%s, %s, %s, %s, %s)"
                    guvi.execute(insert_one, (state_list,
                                              year_list,
                                              quarter,
                                              district_list,
                                              d["data"]["hoverData"][district_list]["registeredUsers"],
                                              d["data"]["hoverData"][district_list]["appOpens"]))
                    selva.commit()


def top_transaction(top_trans):
    guvi.execute("DELETE FROM Top_Trans")
    selva.commit()
    states = os.listdir(top_trans)
    for state_list in states:
        year = os.listdir(f"{top_trans}\{state_list}")
        for year_list in year:
            quar = os.listdir(f"{top_trans}\{state_list}\{year_list}")
            for quar_list in quar:
                # print(f"{map_trans}\{state_list}\{year_list}\{quar_list}") --> Verification line
                file = f"{top_trans}\{state_list}\{year_list}\{quar_list}"
                dt = open(file, 'r')
                d = json.load(dt)
                quarter = "Q" + quar_list.strip('.json')
                sub_range = len(d["data"]["pincodes"])
                for i in range(sub_range):
                    pin = d["data"]["pincodes"][i]["entityName"]
                    if pin is not None:
                        guvi.execute(f"select district from pincode where pincode = {pin}")
                        y = guvi.fetchall()
                        if len(y) == 0:
                            pass
                        else:
                            District = y[0][0]
                            insert_one = "INSERT INTO Top_Trans VALUES (%s, %s, %s, %s, %s, %s, %s)"
                            guvi.execute(insert_one, (state_list,
                                                      year_list,
                                                      quarter,
                                                      pin,
                                                      District,
                                                      d["data"]["pincodes"][i]["metric"]["count"],
                                                      d["data"]["pincodes"][i]["metric"]["amount"]
                                                      ))
                        selva.commit()


def top_users(top_user):
    guvi.execute("DELETE FROM Top_users")
    selva.commit()
    states = os.listdir(top_user)
    for state_list in states:
        year = os.listdir(f"{top_user}\{state_list}")
        for year_list in year:
            quar: list[str] = os.listdir(f"{top_user}\{state_list}\{year_list}")
            for quar_list in quar:
                # print(f"{top_user}\{state_list}\{year_list}\{quar_list}")
                file = f"{top_user}\{state_list}\{year_list}\{quar_list}"
                dt = open(file, 'r')
                d = json.load(dt)
                quarter = "Q" + quar_list.strip('.json')
                sub_range = len(d["data"]["pincodes"])
                for i in range(sub_range):
                    pin = d["data"]["pincodes"][i]["name"]
                    if pin is not None:
                        guvi.execute(f"select district from pincode where pincode = {pin}")
                        y = guvi.fetchall()
                        if len(y) == 0:
                            pass
                        else:
                            district = y[0][0]
                            insert_one = "INSERT INTO Top_users VALUES (%s, %s, %s, %s, %s, %s)"
                            guvi.execute(insert_one, (state_list,
                                                      year_list,
                                                      quarter,
                                                      pin,
                                                      district,
                                                      d["data"]["pincodes"][i]["registeredUsers"]
                                                      ))
                        selva.commit()
def state_geo():
    state_geo = pd.read_csv("D:\Python\PycharmProjects\pythonProject\PhonepePulse\State_Geo.csv")
    for index, i in state_geo.iterrows():
        insert_one = "INSERT INTO state_Geo VALUES (%s, %s, %s)"
        guvi.execute(insert_one, (i['State'], i['Latitude'], i['Longitude']))
        selva.commit()

def district_geo():
    district_geo = pd.read_csv("D:\Python\PycharmProjects\pythonProject\PhonepePulse\District_Geo.csv")
    for index, i in district_geo.iterrows():
        insert_one = "INSERT INTO District_Geo VALUES (%s, %s, %s, %s)"
        guvi.execute(insert_one, (i['State'], i['District'], i['Latitude'], i['Longitude']))
        selva.commit()

def update_database():
    aggr_trans = r"D:\Python\PycharmProjects\pythonProject\pulse\data\aggregated\transaction\country\india\state"
    aggr_user = r"D:\Python\PycharmProjects\pythonProject\pulse\data\aggregated\user\country\india\state"
    map_trans = r"D:\Python\PycharmProjects\pythonProject\pulse\data\map\\transaction\hover\country\india\state"
    map_user = r"D:\Python\PycharmProjects\pythonProject\pulse\data\map\user\hover\country\india\state"
    top_trans = r"D:\Python\PycharmProjects\pythonProject\pulse\data\\top\\transaction\country\india\state"
    top_user = r"D:\Python\PycharmProjects\pythonProject\pulse\data\\top\user\country\india\state"
    aggregated_transaction(aggr_trans)
    aggregated_user(aggr_user)
    map_transaction(map_trans)
    map_users(map_user)
    top_transaction(top_trans)
    top_users(top_user)

state_name_correction = {'All':'All', 'andaman-&-nicobar-islands': 'Andaman & Nicobar','andhra-pradesh':'Andhra Pradesh', 'arunachal-pradesh':'Arunachal Pradesh',
       'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarh', 'chhattisgarh':'Chhattisgarh',
       'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi': 'Delhi', 'goa':'Goa', 'gujarat': 'Gujarat',
       'haryana':'Haryana','himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand',
       'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh',
       'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland',
       'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim',
       'tamil-nadu': 'Tamil Nadu', 'telangana':'Telangana', 'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh',
       'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'}


st.set_page_config(page_title="Phonepe Pulse Data Visualization", page_icon="",layout="wide")
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 0rem;
    }
    </style>
    """,
    unsafe_allow_html=True
)
st.title("Phonepe Pulse Data Visualization")
selected = option_menu(
        menu_title=None,  # required
        options=["Home", "Chart", "Map", "Top 10 Info", "About"],  # required
        icons=["house", "pie-chart", "geo-alt", "trophy", "envelope"],  # optional
        menu_icon="cast",  # optional
        default_index=0,  # optional
styles = {"nav-link": {"--hover-color": "grey"}},
        orientation="horizontal",
    )
# Homepage content upon clicking Home option in the tab
if selected == "Home":
    st.markdown('__<p style="font-family: verdana; text-align:center; font-size: 30px; color: #FAA026">Phonepe and It\'s Journey</P>__',
                unsafe_allow_html=True)

    st.markdown("""Overview of Phonepe's 8 years of journey in below points:

    * PhonePe is a digital payments platform that allows users to make various kinds of transactions like money 
      transfers, bill payments, recharges, and online shopping.

    * It was founded in 2015 by Sameer Nigam, Rahul Chari, and Burzin Engineer, and is headquartered in Bengaluru, India.

    * PhonePe is owned by Flipkart, one of the largest e-commerce companies in India, and is one of the leading digital payments platforms in the country.

    * The app is available for both Android and iOS users, and has over 300 million registered users as of 2021.

    * PhonePe supports multiple payment modes like UPI (Unified Payments Interface), credit/debit cards, and wallets.

    * The UPI payment mode is one of the most popular modes on PhonePe, allowing users to send and receive money instantly from their bank account without the need to enter bank details every time.

    * PhonePe offers a wide range of services including bill payments, mobile recharges, DTH recharges, electricity and water bill payments, and gas bill payments.

    * The app also allows users to book tickets for movies, flights, and hotels, and shop online from various e-commerce websites.

    * PhonePe has also introduced its own super app which allows users to access various services like shopping, food delivery, and more from within the app itself.

    * PhonePe has received several awards for its innovative and user-friendly platform, including the Best Payments App at the India Digital Awards in 2020 and 2021.

    * The PhonePe Pulse Dataset API is a first-of-its-kind open data initiative in the payments space, that demystify the what, why and how of digital payments in India.""")

# Homepage content upon clicking Home option in the tab
elif selected == "Chart":
    st.subheader("Aggregated information")
    col1,col2,col3, col4 = st.columns(4)
    with col3:
        quar_options = ["All", "1st Quarter", "2nd Quarter", "3rd Quarter", "4th Quarter"]
        selected_option = st.selectbox("Quarter:", quar_options)

        if selected_option == "1st Quarter":
            Quarter = "Q1"
        elif selected_option == "2nd Quarter":
            Quarter = "Q2"
        elif selected_option == "3rd Quarter":
            Quarter = "Q3"
        elif selected_option == "4th Quarter":
            Quarter = "Q4"
        else:
            Quarter = None
    with col2:
        year_options = ["All", 2018, 2019, 2020, 2021, 2022]
        selected_option = st.selectbox("Year:", year_options)

        if selected_option == "All":
            Year = None

        else:
            Year = selected_option

    with col1:
        state_options = list(state_name_correction.keys())
        selected_option = st.selectbox("State:", state_options)

        if selected_option == "All":
            State = None
        else:
            State = selected_option.lower()
    with col4:
        search = st.button("Search")


    if search:
        # data retrieving from sql Aggregate transaction table to python for chart plotting
        table = 'Agg_Trans'
        y = int()
        if State is None and Year is None and Quarter is None:
            guvi.execute(f"select * from {table}")
            y_at = guvi.fetchall()
        elif State is None and Year is not None and Quarter is None:
            guvi.execute(f"select * from {table} where year = {Year}")
            y_at = guvi.fetchall()
        elif State is None and Year is None and Quarter is not None:
            guvi.execute(f"select * from {table} where quarter = '{Quarter}'")
            y_at = guvi.fetchall()
        elif State is not None and Year is None and Quarter is None:
            guvi.execute(f"select * from {table} where state = '{State}'")
            y_at = guvi.fetchall()
        elif State is not None and Year is not None and Quarter is None:
            guvi.execute(f"select * from {table} where state = '{State}' and year = '{Year}'")
            y_at = guvi.fetchall()
        elif State is not None and Year is None and Quarter is not None:
            guvi.execute(f"select * from {table} where state = '{State}' and quarter = '{Quarter}'")
            y_at = guvi.fetchall()
        elif State is None and Year is not None and Quarter is not None:
            guvi.execute(f"select * from {table} where year = '{Year}' and quarter = '{Quarter}'")
            y_at = guvi.fetchall()
        else:
            guvi.execute(f"select * from {table} where state = '{State}' and year = '{Year}' and quarter = '{Quarter}'")
            y_at = guvi.fetchall()
        df_at = pd.DataFrame(y_at, columns=['state', 'year', 'quarter', 'date_from', 'date_to', 'transaction_type', 'transaction_count', 'transaction_amount'])
        df_at = df_at.reset_index(drop=True)
        # st.dataframe(df_at, width=1000, height=500)


        fig_at = px.histogram(df_at, x="quarter", y="transaction_amount", color="transaction_type",
                           color_discrete_sequence=['green', '#FF5733', 'white', 'yellow', 'red'],
                           title="Aggregated Transancation Details", barmode="group")
        fig_at.update_layout(title_x=0.45, title_y=0.85)
        fig_at.update_xaxes(title='Transaction Type')
        fig_at.update_yaxes(title='Transaction Amount')

        table = 'agg_user'
        y_au = int()
        if State is None and Year is None and Quarter is None:
            guvi.execute(f"select * from {table}")
            y_au = guvi.fetchall()
        elif State is None and Year is not None and Quarter is None:
            guvi.execute(f"select * from {table} where year = {Year}")
            y_au = guvi.fetchall()
        elif State is None and Year is None and Quarter is not None:
            guvi.execute(f"select * from {table} where quarter = '{Quarter}'")
            y_au = guvi.fetchall()
        elif State is not None and Year is None and Quarter is None:
            guvi.execute(f"select * from {table} where state = '{State}'")
            y_au = guvi.fetchall()
        elif State is not None and Year is not None and Quarter is None:
            guvi.execute(f"select * from {table} where state = '{State}' and year = '{Year}'")
            y_au = guvi.fetchall()
        elif State is not None and Year is None and Quarter is not None:
            guvi.execute(f"select * from {table} where state = '{State}' and quarter = '{Quarter}'")
            y_au = guvi.fetchall()
        elif State is None and Year is not None and Quarter is not None:
            guvi.execute(f"select * from {table} where year = '{Year}' and quarter = '{Quarter}'")
            y_au = guvi.fetchall()
        else:
            guvi.execute(f"select * from {table} where state = '{State}' and year = '{Year}' and quarter = '{Quarter}'")
            y_au = guvi.fetchall()
        df_au = pd.DataFrame(y_au, columns=['state', 'year', 'quarter', 'device_brand', 'device_count','device_percentage'])
        df_au = df_au.reset_index(drop=True).sort_values(by='device_count', ascending=False)
        # st.dataframe(df_at, width=1000, height=500)

        fig_au = px.histogram(df_au, x="quarter", y="device_count", color="device_brand",
                              title="Aggregated User Details", barmode="group")
        fig_au.update_layout(title_x=0.45, title_y=0.85,xaxis={'categoryorder': 'total descending'})
        fig_au.update_xaxes(title='Quarter(s)')
        fig_au.update_yaxes(title='Device count')

        col1 , col2 = st.columns([2,1])
        with col1:
            st.plotly_chart(fig_at, theme="streamlit", use_container_width=True)
        with col2:
            st.markdown('__<p style="text-align:center;font-size: 20px; color: #FAA026">Transaction Summary</P>__', unsafe_allow_html=True)
            if (df_at['transaction_amount'].sum()%10000000) == 0:
                trans_amt = '{:.2f}'.format(df_at['transaction_amount'].sum()/100000) + " Lacs"
            else:
                trans_amt = '{:.2f}'.format(df_at['transaction_amount'].sum() / 10000000) + " Cr"
            col3, col4, col5 = st.columns([2.5,0.5,2])
            with col3:
                st.markdown('Transaction Amount: ', unsafe_allow_html=True)
            with col5:
                st.markdown(f"₹ {trans_amt}")
            col3, col4, col5 = st.columns([2, 1, 2])
            with col3:
                st.markdown(f"Transaction Count")
            with col5:
                st.markdown(df_at['transaction_count'].sum())
            col3, col4, col5 = st.columns([2, 1, 2])
            with col3:
                st.markdown(f"Ave Amount/Trans.")
            with col5:
                st.markdown('{:.2f}'.format(df_at['transaction_amount'].sum()/df_at['transaction_count'].sum()))

        col1, col2 = st.columns([2, 1])
        with col1:
            st.plotly_chart(fig_au, theme="streamlit", use_container_width=True)
        with col2:
            st.markdown('__<p style="text-align:center;font-size: 20px; color: #FAA026">Device Summary</P>__',
                        unsafe_allow_html=True)
            col3, col4, col5 = st.columns([2, 1, 2])
            with col3:
                st.markdown(f"Device Count")
            with col5:
                st.markdown(df_au['device_count'].sum())
            col3, col4, col5 = st.columns([2, 1, 2])
            with col3:
                st.markdown(f"User Percentage")
            with col5:
                st.markdown('{:.2f}'.format(df_au['device_percentage'].sum()))
            st.markdown('__<p style="text-align:center;font-size: 20px; color: #FAA026">Top 5 Mobile Devices</P>__',
                    unsafe_allow_html=True)
            col3, col4, col5 = st.columns([0.5, 2, 0.5])
            with col4:
                df_au_t5 = df_au.groupby('device_brand')['device_count'].sum().sort_values(ascending=False)
                st.dataframe(df_au_t5.head())

elif selected == "Map":

    state_option = list(state_name_correction.keys())
    prev_y_mt = select_state = select_year = select_quarter = 'All'
    col1, col2,col6 = st.columns(3)
    with col1:
        col3, col4 = st.columns([2,1])
        with col3:
            st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">State</P>__',
                        unsafe_allow_html=True)
            select_state = st.selectbox("Select State",state_option,label_visibility="hidden")

    with col2:
        col3, col4, col5 = st.columns(3)
        with col3:
            st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">Year</P>__',
                        unsafe_allow_html=True)
            select_year = st.selectbox("Year",('All',2018,2019,2020,2021,2022),label_visibility="hidden")

    with col6:
        col1, col4, col5 = st.columns(3)
        with col1:
            st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">Quarter</P>__',
                        unsafe_allow_html=True)
            select_quarter = st.selectbox("Quarter", ('All','Q1','Q2','Q3','Q4'), label_visibility="hidden")

    table = 'Map_Trans'
    if select_state == 'All' and select_year == 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table}")
        y_mt = guvi.fetchall()
    elif select_state == 'All' and select_year != 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table} where year = {select_year}")
        y_mt = guvi.fetchall()
    elif select_state == 'All' and select_year == 'All' and select_quarter != 'All':
        guvi.execute(f"select * from {table} where quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
    elif select_state == 'All' and select_year != 'All' and select_quarter != 'All':
        guvi.execute(f"select * from {table} where year = '{select_year}' and quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
    elif select_state != 'All' and select_year == 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table} where state = '{select_state}'")
        y_mt = guvi.fetchall()
    elif select_state != 'All' and select_year != 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table} where state = '{select_state}' and year = '{select_year}'")
        y_mt = guvi.fetchall()
    elif select_state != 'All' and select_year == 'All' and select_quarter != 'All':
        guvi.execute(f"select * from {table} where state = '{select_state}' and quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
    else:
        guvi.execute(
            f"select * from {table} where state = '{select_state}' and year = '{select_year}' and quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
    df_mt = pd.DataFrame(y_mt, columns=['State', 'Year', 'Quarter', 'District', 'Total_Count','Total_Amount'])
    df_mt = df_mt.reset_index(drop=True)
    st.write(f"Geomap of {select_state} for the {select_year} year - {select_quarter} quarter")

    # geo-Map section for Transaction
    if select_state == 'All':
        st.markdown('__<p style="text-align:center; font-size: 20px; color: #FAA026">Geo Map based on Transaction</P>__',
                    unsafe_allow_html=True)
        df_mt['State'] = df_mt['State'].replace(state_name_correction)
        guvi.execute("select * from state_geo")
        y = guvi.fetchall()
        state_geo_df = pd.DataFrame(y,columns=['State', 'Latitude', 'Longitude'])
        nation_choro = df_mt[['State', 'Total_Amount', 'Total_Count']].groupby(['State']).sum().reset_index()
        nation_choro = pd.merge(nation_choro, state_geo_df, "outer", "State")
        fig_1 = px.scatter_geo(nation_choro, lon=nation_choro['Longitude'], lat=nation_choro['Latitude'],
                              hover_name='State', text=nation_choro['State'],
                              hover_data=['Total_Count', 'Total_Amount'], size_max=30)
        fig_1.update_traces(marker=dict(color="black", size=5))
        fig = px.choropleth(nation_choro,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Total_Amount',
                            hover_data=['State', 'Total_Count'],
                            color_continuous_scale='Turbo')
        fig.update_geos(fitbounds='locations', visible=False)
        fig.add_trace(fig_1.data[0])
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},geo=dict(bgcolor='#00172B'),height=500, width=1200)
        st.plotly_chart(fig)

    else:
        st.markdown(
            '__<p style="text-align:center; font-size: 20px; color: #FAA026">Geo Map based on Transaction</P>__',
            unsafe_allow_html=True)
        guvi.execute(f"select * from district_geo where state = '{select_state}'")
        y = guvi.fetchall()
        district_geo_df = pd.DataFrame(y, columns=['State', 'District', 'Latitude', 'Longitude'])
        df_mt1 = df_mt.copy()
        district_choro1 = pd.merge(df_mt1, district_geo_df, 'outer', ['State', 'District'])
        df_mt['State'] = df_mt['State'].replace(state_name_correction)
        district_choro = pd.merge(df_mt, district_geo_df, 'outer', ['State', 'District'])
        fig_2 = px.scatter_geo(district_choro1, lon=district_choro1['Longitude'], lat=district_choro1['Latitude'],
                              hover_name='District',
                              hover_data=['Total_Count', 'Total_Amount', 'Year', 'Quarter'], size_max=30,)
        fig_2.update_traces(marker=dict(color="red", size=5))
        fig = px.choropleth(district_choro,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Total_Amount',
                            hover_data=['District', 'Total_Count'],
                            color_continuous_scale='Tealgrn')
        fig.update_geos(fitbounds='locations', visible=False)
        fig.add_trace(fig_2.data[0])
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, geo=dict(bgcolor='#00172B'),height=500, width=1200)
        st.plotly_chart(fig,use_container_width=True)

    table = 'Map_users'
    if select_state == 'All' and select_year == 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table}")
        y_mt = guvi.fetchall()
    elif select_state == 'All' and select_year != 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table} where year = {select_year}")
        y_mt = guvi.fetchall()
        guvi.execute(f"select total_user, app_opened from {table} where year = {select_year-1}")
        prev_y_mt = guvi.fetchall()
    elif select_state == 'All' and select_year == 'All' and select_quarter != 'All':
        guvi.execute(f"select * from {table} where quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
    elif select_state == 'All' and select_year != 'All' and select_quarter != 'All':
        guvi.execute(f"select * from {table} where year = '{select_year}' and quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
        guvi.execute(f"select total_user, app_opened from {table} where year = '{select_year-1}' and quarter = '{select_quarter}'")
        prev_y_mt = guvi.fetchall()
    elif select_state != 'All' and select_year == 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table} where state = '{select_state}'")
        y_mt = guvi.fetchall()
    elif select_state != 'All' and select_year != 'All' and select_quarter == 'All':
        guvi.execute(f"select * from {table} where state = '{select_state}' and year = {select_year}")
        y_mt = guvi.fetchall()
        guvi.execute(f"select total_user, app_opened from {table} where state = '{select_state}' and year = {select_year-1}")
        prev_y_mt = guvi.fetchall()
    elif select_state != 'All' and select_year == 'All' and select_quarter != 'All':
        guvi.execute(f"select * from {table} where state = '{select_state}' and quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
    else:
        guvi.execute(
            f"select * from {table} where state = '{select_state}' and year = '{select_year}' and quarter = '{select_quarter}'")
        y_mt = guvi.fetchall()
        guvi.execute(
            f"select total_user, app_opened from {table} where state = '{select_state}' and year = '{select_year - 1}' and quarter = '{select_quarter}'")
        prev_y_mt = guvi.fetchall()

    df_mt = pd.DataFrame(y_mt, columns=['State', 'Year', 'Quarter', 'District', 'User_Count','App_Opened'])
    df_mt = df_mt.reset_index(drop=True)

    # geo-Map section for User
    if select_state == 'All':
        st.markdown('__<p style="text-align:Center; font-size: 20px; color: #FAA026">Geo Map based on User</P>__',
                    unsafe_allow_html=True)
        df_mt['State'] = df_mt['State'].replace(state_name_correction)
        guvi.execute("select * from state_geo")
        y = guvi.fetchall()
        state_geo_df = pd.DataFrame(y,columns=['State', 'Latitude', 'Longitude'])
        nation_choro = df_mt[['State', 'User_Count', 'App_Opened']].groupby(['State']).sum().reset_index()
        nation_choro = pd.merge(nation_choro, state_geo_df, "outer", "State")
        fig_1 = px.scatter_geo(nation_choro, lon=nation_choro['Longitude'], lat=nation_choro['Latitude'],
                              hover_name='State', text=nation_choro['State'],
                              hover_data=['User_Count', 'App_Opened'], size_max=30)
        fig_1.update_traces(marker=dict(color="black", size=5))
        fig = px.choropleth(nation_choro,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='User_Count',
                            hover_data=['State', 'App_Opened'],
                            color_continuous_scale='Turbo')
        fig.update_geos(fitbounds='locations', visible=False)
        fig.add_trace(fig_1.data[0])
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0},geo=dict(bgcolor='#00172B'),height=500, width=1200)
        st.plotly_chart(fig)
        current_user_count = df_mt['User_Count'].sum()
        current_app_Opened = df_mt['App_Opened'].sum()

    else:
        st.markdown('__<p style="text-align:center; font-size: 20px; color: #FAA026">Geo Map based on User</P>__',
                    unsafe_allow_html=True)
        guvi.execute(f"select * from district_geo where state = '{select_state}'")
        y = guvi.fetchall()
        district_geo_df = pd.DataFrame(y, columns=['State', 'District', 'Latitude', 'Longitude'])
        df_mt1 = df_mt.copy()
        district_choro1 = pd.merge(df_mt1, district_geo_df, 'outer', ['State', 'District'])
        df_mt['State'] = df_mt['State'].replace(state_name_correction)
        district_choro = pd.merge(df_mt, district_geo_df, 'outer', ['State', 'District'])
        fig_2 = px.scatter_geo(district_choro1, lon=district_choro1['Longitude'], lat=district_choro1['Latitude'],
                               hover_name='District',
                               hover_data=['User_Count', 'App_Opened', 'Year', 'Quarter'], size_max=30, )
        fig_2.update_traces(marker=dict(color="red", size=5))
        fig = px.choropleth(district_choro,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='User_Count',
                            hover_data=['District', 'App_Opened'],
                            color_continuous_scale='Tealgrn')
        fig.update_geos(fitbounds='locations', visible=False)
        fig.add_trace(fig_2.data[0])
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, geo=dict(bgcolor='#00172B'),height=500, width=1200)
        st.plotly_chart(fig,use_container_width=True)


elif selected == "Top 10 Info":
    col1, col2, col3 = st.columns([2, 0.5, 2])
    with col1:
        st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">Year</P>__',
                    unsafe_allow_html=True)
        select_year = st.radio("Year", ('All', 2018, 2019, 2020, 2021, 2022), label_visibility="hidden",
                               horizontal=True)
    with col3:
        st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">Quarter</P>__',
                    unsafe_allow_html=True)
        select_quarter = st.radio("Quarter", ('All', 'Q1', 'Q2', 'Q3', 'Q4'), label_visibility="hidden",
                                  horizontal=True)

    selected_year = select_year if select_year != 'All' else select_year is None
    selected_quarter = select_quarter if select_quarter != 'All' else select_quarter is None

    df_tt_st = df_tt_dt = df_tt_pc = df_tt_con = []
    df_tu_st = df_tu_dt = df_tu_pc = df_tu_con = []

    if selected_year is False and selected_quarter is False:
        guvi.execute(f"SELECT 'States' as division, state, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE state IN (SELECT state FROM top_trans "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_st = guvi.fetchall()
        df_tt_st = pd.DataFrame(y_tt_st,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE district IN (SELECT district FROM top_trans "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_dt = guvi.fetchall()
        df_tt_dt = pd.DataFrame(y_tt_dt,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
                     f"SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE pincode IN (SELECT pincode FROM top_trans "
                     f"GROUP BY pincode "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY pincode "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_pc = guvi.fetchall()
        df_tt_pc = pd.DataFrame(y_tt_pc,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'States' as division, state, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE state IN (SELECT state FROM top_users "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_st = guvi.fetchall()
        df_tu_st = pd.DataFrame(y_tu_st,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE district IN (SELECT district FROM top_users "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_dt = guvi.fetchall()
        df_tu_dt = pd.DataFrame(y_tu_dt,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(
            f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
            f"SUM(total_count) AS total_count "
            f"FROM top_users "
            f"WHERE pincode IN (SELECT pincode FROM top_users "
            f"GROUP BY pincode "
            f"ORDER BY SUM(total_count) DESC "
            f"LIMIT 10) "
            f"GROUP BY pincode "
            f"ORDER BY total_count DESC "
            f"LIMIT 10")
        y_tu_pc = guvi.fetchall()
        df_tu_pc = pd.DataFrame(y_tu_pc,
                                columns=['division', 'Name', 'total_count'])

    elif selected_year is not False and selected_quarter is False:
        guvi.execute(f"SELECT 'States' as division, state, SUM(total_amount) AS total_amount FROM top_trans "
                     f"WHERE year = {selected_year} and state IN (SELECT state FROM top_trans "
                     f"WHERE year = {selected_year} "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_st = guvi.fetchall()
        df_tt_st = pd.DataFrame(y_tt_st,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE year = {selected_year} and district IN (SELECT district FROM top_trans "
                     f"WHERE year = {selected_year} "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_dt = guvi.fetchall()
        df_tt_dt = pd.DataFrame(y_tt_dt,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
                     f"SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE year = {selected_year} and pincode IN (SELECT pincode FROM top_trans "
                     f"WHERE year = {selected_year} "
                     f"GROUP BY pincode "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY pincode "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_pc = guvi.fetchall()
        df_tt_pc = pd.DataFrame(y_tt_pc,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'States' as division, state, SUM(total_count) AS total_count FROM top_users "
                     f"WHERE year = {selected_year} and state IN (SELECT state FROM top_users "
                     f"WHERE year = {selected_year} "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_st = guvi.fetchall()
        df_tu_st = pd.DataFrame(y_tu_st,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE year = {selected_year} and district IN (SELECT district FROM top_users "
                     f"WHERE year = {selected_year} "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_dt = guvi.fetchall()
        df_tu_dt = pd.DataFrame(y_tu_dt,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(
            f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
            f"SUM(total_count) AS total_count "
            f"FROM top_users "
            f"WHERE year = {selected_year} and pincode IN (SELECT pincode FROM top_users "
            f"WHERE year = {selected_year} "
            f"GROUP BY pincode "
            f"ORDER BY SUM(total_count) DESC "
            f"LIMIT 10) "
            f"GROUP BY pincode "
            f"ORDER BY total_count DESC "
            f"LIMIT 10")
        y_tu_pc = guvi.fetchall()
        df_tu_pc = pd.DataFrame(y_tu_pc,
                                columns=['division', 'Name', 'total_count'])

    elif selected_year is False and selected_quarter is not False:
        guvi.execute(f"SELECT 'States' as division, state, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE quarter = '{selected_quarter}' and state IN (SELECT state FROM top_trans "
                     f"WHERE quarter = '{selected_quarter}' "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_st = guvi.fetchall()
        df_tt_st = pd.DataFrame(y_tt_st,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE quarter = '{selected_quarter}' and district IN (SELECT district FROM top_trans "
                     f"WHERE quarter = '{selected_quarter}' "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_dt = guvi.fetchall()
        df_tt_dt = pd.DataFrame(y_tt_dt,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
                     f"SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE quarter = '{selected_quarter}' and pincode IN (SELECT pincode FROM top_trans "
                     f"WHERE quarter = '{selected_quarter}' "
                     f"GROUP BY pincode "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY pincode "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_pc = guvi.fetchall()
        df_tt_pc = pd.DataFrame(y_tt_pc,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'States' as division, state, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE quarter = '{selected_quarter}' and state IN (SELECT state FROM top_users "
                     f"WHERE quarter = '{selected_quarter}' "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_st = guvi.fetchall()
        df_tu_st = pd.DataFrame(y_tu_st,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE quarter = '{selected_quarter}' and district IN (SELECT district FROM top_users "
                     f"WHERE quarter = '{selected_quarter}' "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_dt = guvi.fetchall()
        df_tu_dt = pd.DataFrame(y_tu_dt,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(
            f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
            f"SUM(total_count) AS total_count "
            f"FROM top_users "
            f"WHERE quarter = '{selected_quarter}' and pincode IN (SELECT pincode FROM top_users "
            f"WHERE quarter = '{selected_quarter}' "
            f"GROUP BY pincode "
            f"ORDER BY SUM(total_count) DESC "
            f"LIMIT 10) "
            f"GROUP BY pincode "
            f"ORDER BY total_count DESC "
            f"LIMIT 10")
        y_tu_pc = guvi.fetchall()
        df_tu_pc = pd.DataFrame(y_tu_pc,
                                columns=['division', 'Name', 'total_count'])

    elif selected_year is not False and selected_quarter is not False:
        guvi.execute(f"SELECT 'States' as division, state, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' and "
                     f"state IN (SELECT state FROM top_trans "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_st = guvi.fetchall()
        df_tt_st = pd.DataFrame(y_tt_st,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' and "
                     f"district IN (SELECT district FROM top_trans "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_dt = guvi.fetchall()
        df_tt_dt = pd.DataFrame(y_tt_dt,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
                     f"SUM(total_amount) AS total_amount "
                     f"FROM top_trans "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' and "
                     f"pincode IN (SELECT pincode FROM top_trans "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' "
                     f"GROUP BY pincode "
                     f"ORDER BY SUM(total_amount) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY pincode "
                     f"ORDER BY total_amount DESC "
                     f"LIMIT 10")
        y_tt_pc = guvi.fetchall()
        df_tt_pc = pd.DataFrame(y_tt_pc,
                                columns=['division', 'Name', 'total_amount'])

        guvi.execute(f"SELECT 'States' as division, state, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' and "
                     f"state IN (SELECT state FROM top_users "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' "
                     f"GROUP BY state "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY state "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_st = guvi.fetchall()
        df_tu_st = pd.DataFrame(y_tu_st,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(f"SELECT 'Districts' as division, district, SUM(total_count) AS total_count "
                     f"FROM top_users "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' and "
                     f"district IN (SELECT district FROM top_users "
                     f"WHERE year = {selected_year} and quarter = '{selected_quarter}' "
                     f"GROUP BY district "
                     f"ORDER BY SUM(total_count) DESC "
                     f"LIMIT 10) "
                     f"GROUP BY district "
                     f"ORDER BY total_count DESC "
                     f"LIMIT 10")
        y_tu_dt = guvi.fetchall()
        df_tu_dt = pd.DataFrame(y_tu_dt,
                                columns=['division', 'Name', 'total_count'])

        guvi.execute(
            f"SELECT 'Pincodes' as division, CAST(LPAD(pincode::text, 6, ' ') AS TEXT) AS pincode_text, "
            f"SUM(total_count) AS total_count "
            f"FROM top_users "
            f"WHERE year = {selected_year} and quarter = '{selected_quarter}' and "
            f"pincode IN (SELECT pincode FROM top_users "
            f"WHERE year = {selected_year} and quarter = '{selected_quarter}' "
            f"GROUP BY pincode "
            f"ORDER BY SUM(total_count) DESC "
            f"LIMIT 10) "
            f"GROUP BY pincode "
            f"ORDER BY total_count DESC "
            f"LIMIT 10")
        y_tu_pc = guvi.fetchall()
        df_tu_pc = pd.DataFrame(y_tu_pc,
                                columns=['division', 'Name', 'total_count'])
    df_tt_con = pd.concat([df_tt_dt, df_tt_st, df_tt_pc], axis=0).reset_index()
    df_tu_con = pd.concat([df_tu_dt, df_tu_st, df_tu_pc], axis=0).reset_index()

    # Top 10 Transaction details in sunburst and in dataframe
    col1,col2 = st.columns([3.5,2])
    with col1:
        fig_tt = px.sunburst(df_tt_con, path = ['division', 'Name'],
                             values = 'total_amount',
                             color = 'division',
                             color_discrete_map={'States': '#65FAAB','Districts': '#FAA026', 'Pincodes':'#C3FA26'},
                             hover_data = ['division', 'Name', 'total_amount'],
                             title = "Top 10 Transaction Details"
                             )
        fig_tt.update_layout(title_x=0.15, title_y=0.99, margin = dict(t=20, l=10, r=10, b=0))
        st.plotly_chart(fig_tt, theme="streamlit", use_container_width=True)
    with col2:
        # st.markdown('__<p style="text-align:left; font-size: 20px; color: white">Segment</P>__',
        # unsafe_allow_html=True)
        segment = st.radio("Segment", ('State', 'District', 'Pincode'), label_visibility="visible",
                           horizontal=True)
        if segment == 'State':
            df_tt_st['Name'] = df_tt_st['Name'].replace(state_name_correction)
            df = df_tt_st.groupby('Name')['total_amount'].sum().sort_values(ascending=False)
            st.dataframe(df, use_container_width=True)
        elif segment == 'District':
            df = df_tt_dt.groupby('Name')['total_amount'].sum().sort_values(ascending=False)
            st.dataframe(df, use_container_width=True)
        elif segment == 'Pincode':
            df = df_tt_pc.groupby('Name')['total_amount'].sum().sort_values(ascending=False)
            st.dataframe(df, use_container_width = True)

    # Top user Information details in sunburst and in dataframe
    col1, col2 = st.columns([3.5, 2])
    with col1:
        fig_tt = px.sunburst(df_tu_con, path=['division', 'Name'],
                             values='total_count',
                             color='division',
                             color_discrete_map={'States': '#65FAAB', 'Districts': '#FAA026',
                                                 'Pincodes': '#C3FA26'},
                             hover_data=['division', 'Name', 'total_count'],
                             title="Top 10 User Details"
                             )
        fig_tt.update_layout(title_x=0.15, title_y=0.99, margin=dict(t=20, l=10, r=10, b=0))
        st.plotly_chart(fig_tt, theme="streamlit", use_container_width=True)
    with col2:
        # st.markdown('__<p style="text-align:left; font-size: 20px; color: white">Segment</P>__',
        # unsafe_allow_html=True)
        if segment == 'State':
            df_tu_st['Name'] = df_tu_st['Name'].replace(state_name_correction)
            df = df_tu_st.groupby('Name')['total_count'].sum().sort_values(ascending=False)
            st.dataframe(df, use_container_width=True)
        elif segment == 'District':
            df = df_tu_dt.groupby('Name')['total_count'].sum().sort_values(ascending=False)
            st.dataframe(df, use_container_width=True)
        elif segment == 'Pincode':
            df = df_tu_pc.groupby('Name')['total_count'].sum().sort_values(ascending=False)
            st.dataframe(df, use_container_width=True)

if selected == "About":
    st.markdown('__<p style="text-align:left; font-size: 25px; color: #FAA026">Summary of Data Visualization Project</P>__',
                unsafe_allow_html=True)
    st.write("This data visualization project focused on the user and transaction data of the Phonepe mobile payment app. By taking a copy of data from the Phonepe Pulse git repository, useful information about the behavior and transactions of users was obtained. This data was then consolidated into an interactive dashboard, which offers quick insights into how well the Phonepe app is performing.")
    st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">Applications and Packages Used:</P>__',
                    unsafe_allow_html=True)
    st.write("  * Python")
    st.write("  * PostgresSql")
    st.write("  * Streamlit")
    st.write("  * Pandas")
    st.write("  * Github")
    st.write("  * Plotly")
    st.write("  * Psycopg2")
    st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">For feedback/suggestion, connect with me on</P>__',
                unsafe_allow_html=True)
    st.subheader("LinkedIn")
    st.write("https://www.linkedin.com/in/selvamani-a-795580266/")
    st.subheader("Email ID")
    st.write("selvamani.ind@gmail.com")
    st.subheader("Github")
    st.write("https://github.com/selvamani1992")
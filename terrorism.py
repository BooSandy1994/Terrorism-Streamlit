# from pyspark.sql import SparkSession
# from pyspark.sql.functions import count, desc , col, max , struct
import pandas as pd
import plotly.express as px
# import matplotlib.pyplot as plt
import numpy as np
import streamlit as st
# import geopandas as gpd
# import json

# spark = SparkSession.builder.appName("Spark_App").getOrCreate()
# df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('global.csv')

st.title("Trends in Terrorism around the World:")
st.sidebar.title("Terrorism Trends Options:")

st.markdown("This application has been designed to visualize terrorism trends from 1970. Some of these visualizations are based on big datasets so please wait patiently while they load.")
st.sidebar.markdown("Apply Filters and visualize accordingly.")

d1_url = "d1.csv"
d2_url = "d2.csv"
d3_url = "d3.csv"
d4_url = "d4.csv"
d5_url = "d5.csv"
d6_url = "d6.csv"
d7_url = "d7.csv"
d8_url = "d8.csv"
d11_url = "d11.csv"
d12_url = "d12.csv"
d13_url = "d13.csv"
d14_rurl = "d14_rfails.csv"
d14_furl = "d14_propf.csv"


mapaccess_token = 'pk.eyJ1IjoiYm9vc2FuZHkiLCJhIjoiY2toaXZ4aDNmMWZkazJ5bHVreWlzY2szNCJ9.zHu7eImER0mCoyvny-_30w'

@st.cache(persist=True)

## load the spark session and the d11 part before hand



## Here we plot the d1
def load_d1():
    d1 = pd.read_csv(d1_url)
    return d1

d1 = load_d1()    

st.sidebar.subheader("Most Affected Countries:")

## Now we go and load the d2
def load_d2():
    d2 = pd.read_csv(d2_url)
    d2.columns = ['country', 'regions', 'counts','iso']
    return d2

d2 = load_d2()

def load_d14r():
    d14r = pd.read_csv(d14_rurl)
    return d14r

def load_d14f():
    d14f = pd.read_csv(d14_furl)
    return d14f

d14r = load_d14r()
d14f = load_d14f()        


select = st.sidebar.selectbox('Visualize',['Heat Map','Region Bar Chart', 'Most Affected', 'Failure Proportions'],key = '1')
if not st.sidebar.checkbox('Hide', True,key = '1_1'):
    st.markdown("### Affected Countries by Regions and Territory")
    if select == "Region Bar Chart":
        fig = px.bar(d1,x = 'regions', y = 't_counts',hover_data = ['regions'], title="Affected Regions Of The World", labels = {'regions':'Regions','t_counts':'Instances'})
        fig.update_layout(xaxis_categoryorder = 'total ascending')
        st.plotly_chart(fig)
    elif select == "Heat Map":
        fig = px.choropleth(d2,title="Heat Map of Violent Instances from 1970", locations = 'iso', color = 'counts', hover_name = 'country',projection = 'natural earth', labels = {'counts':'Instances'})
        fig.update_layout(width = 850, height = 600,margin={"r":80,"t":0,"l":0,"b":0})
        st.plotly_chart(fig)  
    elif select == "Most Affected":
        fig = px.bar(d14r, x = 'Country', y = 'Instances', hover_name = 'Country')
        fig.update_layout(title = "15 Most Affected Countries")
        st.plotly_chart(fig)
    elif select == "Failure Proportions":
        fig = px.bar(d14f,x ='Country',y='Prop', hover_name = "Country", hover_data = ['Instances','Total']) 
        fig.update_layout(title = "Ranks by Proportionate Failure rates of the top 15 most affected countries")
        st.plotly_chart(fig)            


## Time Series plotting 
st.sidebar.subheader("Terrorism Trends from 1970 - 2017:")

##load d3 data
def load_d3():
    d3 = pd.read_csv(d3_url)
    return d3

def load_d4():
    d4 = pd.read_csv(d4_url)
    return d4

def load_d5():
    d5 = pd.read_csv(d5_url)
    return d5

d3 = load_d3()
d4 = load_d4()
d5 = load_d5()

s_time = st.sidebar.selectbox('Visualize',['Event Success Trends','Attack Type Trends', 'Terror Origins' ],key = '1')
if not st.sidebar.checkbox('Hide',True,key = '2_1'):
    st.markdown("### Trends in Terrorist Events")
    st.markdown("Scroll over for country view and Click and drag feature available on timeline and map")
    if s_time == "Event Success Trends":
        fig = px.scatter_geo(d3,lat = "latitude", lon = "longitude", hover_name = "city",color = 'success', animation_frame = 'year',title = "From 1970 to 2017", height =700, width = 700, labels = {"year": "Year","success":"Was it Successful?"})
        fig.update_layout(margin = {"r":0,"t":0,"l":0,"b":0}, height = 600, width = 800)
        st.plotly_chart(fig)   
    elif s_time == "Attack Type Trends" :
        fig = px.scatter_geo(d4,lat='lat',lon = 'lon', hover_name = "city", color = "Attack_Type", hover_data = ["city","Attack_Type"], animation_frame='year',title = "From 1970 to 2017",labels = {"year": "Year","Attack_Type":"Type of Attack"} ) 
        fig.update_layout(margin = {"r":0,"t":0,"l":0,"b":0}, height = 700, width = 900)
        st.plotly_chart(fig)       
    elif s_time == "Terror Origins":
        d5.columns = ['year','origin','counts','iso']
        fig = px.scatter_geo(d5, locations = 'iso', hover_name = 'origin', size = 'counts', color = 'counts', hover_data = ['origin'],animation_frame = 'year', animation_group = 'origin', size_max = 60, height = 700, width = 700, labels = {"counts":"Instances", "year":"Year"} , title = "From 1970 to 2017")
        fig.update_layout(margin = {"r":0,"t":0,"l":0,"b":0}, height = 700, width = 900)
        st.plotly_chart(fig)


##Most notorious terrorist groups
st.sidebar.subheader("Most Notorious Terrorist Groups")



# Load data

def load_d6():
    d6 = pd.read_csv(d6_url)
    return d6

def load_d7():
    d7 = pd.read_csv(d7_url)
    return d7

def load_d8():
    d8 = pd.read_csv(d8_url)
    return d8

def load_d12():
    d12 = pd.read_csv(d12_url)
    return d12

d6 = load_d6()
d7 = load_d7()
d8 = load_d8()
d12 = load_d12()

s_notorious = st.sidebar.selectbox('Visualize',['Total Casualties','Favourite Targets', 'Attack Modes','Country Victims' ],key='2')
if not st.sidebar.checkbox("Hide",True,key="2_2"):
    st.markdown("### Terrorist Groups Casualties, Weapons and Favourite Attack Targets")
    if s_notorious == "Total Casualties":
        fig = px.bar(d6,y='groups', x = 'counts', title = "Terrorist Organizations by Casualties since 1970", hover_data = ['groups','origin','counts'], labels = {'counts':'Instances of Violence','groups':'Terrorist Groups'}, height = 700)
        fig.update_layout(yaxis_categoryorder = 'total ascending')
        st.plotly_chart(fig)
    elif s_notorious == "Favourite Targets":
        fig = px.sunburst(d7,path = ['gname','targtype1_txt'], values = 'counts', hover_name = 'full_name', color = 'counts',color_continuous_scale='RdBu', height = 600)
        fig.update_layout(title  = "Favourite targets since 1970 (clickable figure)")
        st.plotly_chart(fig)      
    elif s_notorious ==  "Attack Modes" :
        fig = px.sunburst(d8,path = ['gname','attacktype1_txt','weaptype1_txt'], values = 'counts', hover_name = 'full_name', color = 'counts', color_continuous_scale = 'RdBu', height = 600)
        fig.update_layout(title = " Attack Types Since 1970 (Clickable Figure)")
        st.plotly_chart(fig)  
    elif s_notorious == "Country Victims":
        fig  = px.sunburst(d12,  path = ['groups','country'], values = 'counts', hover_name = 'full_name',labels = {'counts':"Instances of Violence"}, color = 'counts',color_continuous_scale='RdBu', height = 600)  
        fig.update_layout(title = " Famous Terrorist Groups and Their Favourite Victims",margin = {"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig) 


# Bubble charts by countries

st.sidebar.subheader("Top 20 Countries By Instances")

#load data        
def load_d11():
    d11 = pd.read_csv(d11_url)
    return d11

d11 = load_d11()

select_country = st.sidebar.selectbox("Choose Country and Visualize",["Iraq","Pakistan","Afghanistan", "India","Colombia", "Philippines", "Peru","El Salvador", "United Kingdom", "Turkey", "Somalia", "Nigeria", "Thailand", "Yemen", "Spain", "Sri Lanka", "United States","Algeria", "France", "Egypt"],key='3')
if not st.sidebar.checkbox("Hide",True,key='3_1'):
    st.markdown("### Time wise Trends By Country ")
    d11_temp = d11[d11.country_txt == select_country]
    list_ones = np.ones((d11_temp.shape[0],1))
    d11_temp['counts'] = list_ones
    d11_temp = d11_temp.groupby(['iyear','provstate'], as_index = False)['counts'].agg('sum')
    fig = px.scatter(d11_temp,x = 'iyear',y='counts', size = 'counts',color = d11_temp.provstate, hover_name = 'provstate', size_max = 30, range_x = [1955,2020], range_y = [0,500], labels = dict(counts = "Instances of Violence", iyear = "Years", provstate = "Provinces"),height = 700)
    st.plotly_chart(fig)


# multiselect states and countries and slider widget of year

def load_d13():
    d13 = pd.read_csv(d13_url)
    return d13

d13 = load_d13()

st.sidebar.subheader("Top Affected Countries Vulnerability ")

select_level_1 = st.sidebar.selectbox("Choose Country and Visualize",["Iraq","Pakistan","Afghanistan", "India","Colombia", "Philippines", "Peru","El Salvador", "United Kingdom", "Turkey", "Somalia", "Nigeria", "Thailand", "Yemen", "Spain", "Sri Lanka", "United States","Algeria", "France", "Egypt"],key='4')
d13_l1 = d13[d13.Country == select_level_1]
prov_list = ["All"]
prov_list = prov_list + list(d13_l1.Province.unique())
select_level_2 = st.sidebar.multiselect("Pick Provinces",prov_list, key = '4_1')
select_year = st.sidebar.slider("Select Year",min_value = 1970, max_value = 2017,step = 1)
select_style = st.sidebar.selectbox("Map Styles",['dark',"basic",'light', 'outdoors', 'satellite', 'satellite-streets','Mapbox'])


if not st.sidebar.checkbox("Hide",True, key = '4_2'):
    st.markdown("### Vulnerabilties by Country")

    if len(select_level_2) > 0:
        if "All" in select_level_2:
            prov_list.remove("All")
            d13_final = d13_l1[d13_l1.Province.isin(prov_list)]
            d13_final = d13_final[d13_final.Year == select_year]
            if d13_final.shape[0]>0: 
                if select_style == 'Mapbox':
                    fig = px.scatter_mapbox(d13_final, lat="lat", lon="lon", hover_name="Country", hover_data=["Target", "Specific Target"],color_discrete_sequence=['maroon'], zoom=3, height=500)
                    fig.update_layout(mapbox_style='mapbox://styles/boosandy/ckhiw3l7c3sy719np4imr00x4', mapbox_accesstoken=mapaccess_token)
                else:
                    fig = px.scatter_mapbox(d13_final, lat="lat", lon="lon", hover_name="Country", hover_data=["Target", "Specific Target"],color_discrete_sequence=["fuchsia"], zoom=3, height=500)
                    fig.update_layout(mapbox_style=select_style, mapbox_accesstoken=mapaccess_token)
                fig.update_layout(title = " Most Vulnerable Places in the Country By Attacks",margin = {"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig)
            else:
                st.write("Year Data Not Available")    

        else:
            d13_final = d13_l1[d13_l1.Province.isin(select_level_2)]
            d13_final = d13_final[d13_final.Year == select_year]
            if d13_final.shape[0]>0:
                if select_style == 'Mapbox':
                    fig = px.scatter_mapbox(d13_final, lat="lat", lon="lon", hover_name="Country", hover_data=["Target", "Specific Target"],color_discrete_sequence=['maroon'], zoom=3, height=500)
                    fig.update_layout(mapbox_style='mapbox://styles/boosandy/ckhiw3l7c3sy719np4imr00x4', mapbox_accesstoken=mapaccess_token)
                else:
                    fig = px.scatter_mapbox(d13_final, lat="lat", lon="lon", hover_name="Country", hover_data=["Target", "Specific Target"],color_discrete_sequence=["fuchsia"], zoom=3, height=500)
                    fig.update_layout(mapbox_style=select_style, mapbox_accesstoken=mapaccess_token)
                fig.update_layout(title = " Most Vulnerable Places in the Country By Attacks",margin = {"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig)
            else:
                st.write("Year Data Not Available")
    else:
        st.write("Choose Province and Year Options")                

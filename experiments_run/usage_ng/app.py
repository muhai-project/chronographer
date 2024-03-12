# -*- coding: utf-8 -*-
""" 
Streamlit
"""
import os
import folium
import pandas as pd
from datetime import datetime
from settings import FOLDER_PATH
from kglab.helpers.data_load import read_csv
import streamlit as st
from streamlit_folium import st_folium
from streamlit_timeline import timeline

def read_location_df(path):
    df = read_csv(path)
    df = df[(~df.longitude.isna()) & (~df.latitude.isna())]
    return df

def get_color(row, df_helper):
    if df_helper[(df_helper.event == row.event) & (df_helper.location == row.location)].shape[0] > 0:
        row["color"] = '#33FF4F'
    else:
        row["color"] =  "#3933FF"
    return row

def get_timeline_data(df):
    data = {"title": {"text": {"headline": "French Revolution Timeline"}}}
    events = []
    for _, row in df.iterrows():
        start = datetime.strptime(row.start, "%Y-%m-%d")
        end = datetime.strptime(row.end, "%Y-%m-%d")
        events.append({
            "text": {"headline": row.event_clean}, 
            "start_date": {"year": start.year, "month": start.month, "day": start.day},
            "end_date": {"year": end.year, "month": end.month, "day": end.day}
        })
    data["events"] = events
    return data

def get_person_data(df):
    data = {"title": {"text": {"headline": "Louis XVI Timeline"}}}
    events = []
    for event in df.event.unique():
        curr_df = df[df.event == event]
        text = ""
        for _, row in df.iterrows():
            if isinstance(row.sentence_label, str) and row.sentence_label:
                text += f"[Context] {row.sentence_label}\n\n[Role] {row.gfe.split('/')[-1]}\n\n[Surface] {row.role_label}"
        start = datetime.strptime(curr_df.iloc[0].start, "%Y-%m-%d")
        end = datetime.strptime(curr_df.iloc[0].end, "%Y-%m-%d")
        events.append({
            "text": {"headline": curr_df.iloc[0].event_clean, "text": text}, 
            "start_date": {"year": start.year, "month": start.month, "day": start.day},
            "end_date": {"year": end.year, "month": end.month, "day": end.day}
        })
    data["events"] = events
    return data

DF_LOCATION_SEM = read_location_df(os.path.join(FOLDER_PATH, "experiments_run/usage_ng/data/location_sem.csv"))
DF_LOCATION_SEM["color"] = "#3933FF"
DF_LOCATION_SEM_FRAME = read_location_df(os.path.join(FOLDER_PATH, "experiments_run/usage_ng/data/location_sem_frame.csv"))
DF_LOCATION_SEM_FRAME = pd.concat([DF_LOCATION_SEM_FRAME, DF_LOCATION_SEM]).drop_duplicates()
DF_LOCATION_HELPER = pd.merge(DF_LOCATION_SEM_FRAME, DF_LOCATION_SEM, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
DF_LOCATION_SEM_FRAME = DF_LOCATION_SEM_FRAME.apply(lambda row: get_color(row, DF_LOCATION_HELPER), axis=1)
DF_DATES = read_csv(os.path.join(FOLDER_PATH, "experiments_run/usage_ng/data/when_start_end.csv"))

DF_LXVI_SEM = read_csv(os.path.join(FOLDER_PATH, "experiments_run/usage_ng/data/louis_xvi_sem.csv"))
DF_LXVI_SEM_FRAME = read_csv(os.path.join(FOLDER_PATH, "experiments_run/usage_ng/data/louis_xvi_sem_frame.csv"))

st.set_page_config(layout="wide")

@st.cache_data
def build_folium_map(df):
    m = folium.Map()
    for _, row in df.iterrows():
        folium.Marker(
            location=[row.latitude, row.longitude],
            tooltip="Click me!",
            popup="test"
        ).add_to(m)
    return m

st.write("## Comparing locations")

COL_SEM, COL_SEM_FRAME = st.columns(2)
with COL_SEM:
    st.write("SEM")
    # MAP = build_folium_map(df=DF_LOCATION)
    st.map(DF_LOCATION_SEM, longitude="longitude", latitude="latitude", color="color")

with COL_SEM_FRAME:
    st.write("SEM + Frames")
    st.map(DF_LOCATION_SEM_FRAME, longitude="longitude", latitude="latitude", color="color")


DATA_TIMELINE = get_timeline_data(df=DF_DATES)
timeline(DATA_TIMELINE)

COL_PERSON_SEM, COL_PERSON_SEM_FRAME = st.columns(2)
with COL_PERSON_SEM:
    st.write("SEM")
    st.write("Louis XVI's timeline")
    DATA_PERSON_SEM = get_person_data(df=DF_LXVI_SEM)
    timeline(DATA_PERSON_SEM)


with COL_PERSON_SEM_FRAME:
    st.write("SEM + Frame")
    st.write("Louis XVI's timeline")
    DATA_PERSON_SEM_FRAME = get_person_data(df=DF_LXVI_SEM_FRAME)
    timeline(DATA_PERSON_SEM_FRAME)
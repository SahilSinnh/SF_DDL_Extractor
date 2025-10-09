# Contains functions for visualizing the dependency graph.

import streamlit as st
from pyvis.network import Network
from typing import Dict, Set, List, Any
import base64
import os

def get_icon_data_uri(icon_filename: str) -> str:
    #Reads an icon file, encodes it in base64, and returns a data URI.
    
    # Get the absolute path to the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Construct the full path to the icon file
    icon_path = os.path.join(script_dir, "graph-icons", icon_filename)
    
    try:
        with open(icon_path, "rb") as f:
            icon_svg = f.read()
        
        # Encode the SVG content in base64
        encoded_svg = base64.b64encode(icon_svg).decode("utf-8")
        
        # Return the data URI
        return f"data:image/svg+xml;base64,{encoded_svg}"
        
    except FileNotFoundError:
        # Fallback to CDN for missing icons
        return f"https://cdn.jsdelivr.net/npm/@mdi/svg/svg/{icon_filename}"


def create_dependency_graph_figure(objects: List[Dict[str, Any]], deps: Dict[str, Set[str]], selected_schemas: List[str]):
    # Generates an interactive dependency graph using pyvis.
    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", notebook=True, cdn_resources='in_line', directed=True) #type: ignore

    # Filter objects based on selected schemas
    selected_nodes = {
        obj["_CANON_FQN"]
        for obj in objects
        if obj.get("schema") in selected_schemas and obj.get("_CANON_FQN")
    }

    # Add nodes to the graph
    for obj in objects:
        fqn = obj.get("_CANON_FQN")
        if fqn in selected_nodes:
            obj_type = obj.get("object_type", "UNKNOWN")

            # Original color mapping
            color = {
                "DATABASE": "#2196F3",
                "SCHEMA": "#009688",
                "SEQUENCE": "#FFC107",
                "TABLE": "#3F51B5",
                "DYNAMIC TABLE": "#03A9F4",
                "VIEW": "#673AB7",
                "STAGE": "#00BCD4",
                "EXTERNAL TABLE": "#607D8B",
                "FILE FORMAT": "#FF9800",
                "PROCEDURE": "#C2185B",
                "FUNCTION": "#4CAF50",
                "PIPE": "#4682B4",
                "MATERIALIZED VIEW": "#9C27B0",
                "STREAM": "#8BC34A",
                "TASK": "#78909C",
                "MASKING POLICY": "#9E9E9E",
                "TAG": "#E91E63",
            }.get(obj_type, "#90A4AE")
            
            # Map object types to local icon files and create data URIs
            icon_map = {
                "DATABASE": get_icon_data_uri("database.svg"),
                "SCHEMA": get_icon_data_uri("layers.svg"),
                "SEQUENCE": get_icon_data_uri("pound.svg"),
                "TABLE": get_icon_data_uri("table.svg"),
                "DYNAMIC TABLE": get_icon_data_uri("table-refresh.svg"),
                "VIEW": get_icon_data_uri("table-eye.svg"),
                "STAGE": get_icon_data_uri("cloud-upload.svg"),
                "EXTERNAL TABLE": get_icon_data_uri("table-network.svg"),
                "FILE FORMAT": get_icon_data_uri("file-cog.svg"),
                "PROCEDURE": get_icon_data_uri("script-text.svg"),
                "FUNCTION": get_icon_data_uri("function-variant.svg"),
                "PIPE": get_icon_data_uri("pipe.svg"),
                "MATERIALIZED VIEW": get_icon_data_uri("table-star.svg"),
                "STREAM": get_icon_data_uri("view-stream.svg"),
                "TASK": get_icon_data_uri("clipboard-check.svg"),
                "MASKING POLICY": get_icon_data_uri("shield-half-full.svg"),
                "TAG": get_icon_data_uri("tag-multiple.svg"),
            }
            icon_url = icon_map.get(obj_type, get_icon_data_uri("help-circle.svg"))
            
            net.add_node(
                fqn, 
                label=obj.get("object_name"), 
                title=f"{obj_type}\n{fqn}", 
                shape="circularImage", 
                image=icon_url,
                color=color
            )

    # Add edges for the dependencies
    for fqn, dependencies in deps.items():
        if fqn in selected_nodes:
            for dep_fqn in dependencies:
                # Only add the edge if the dependency is also a node in our graph
                if dep_fqn in selected_nodes:
                    net.add_edge(source=fqn, to=dep_fqn)

    # Generate the HTML content directly
    try:
        return net.generate_html()
    except Exception as e:
        st.error(f"Failed to generate dependency graph: {e}")
        return None

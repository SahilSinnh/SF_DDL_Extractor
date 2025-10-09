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
    icon_path = os.path.join(script_dir, "../../assets/icons", icon_filename)
    
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

def _generate_legend_html(schema_colors: Dict[str, str], object_color_map: Dict[str, str], object_icon_map: Dict[str, str]) -> str:
    # Schema Legend
    schema_legend_html = "<h3>Schema Outlines</h3><ul>"
    for schema, color in sorted(schema_colors.items()):
        schema_legend_html += f'''<li>
            <div style="display:flex; align-items:center; margin-bottom: 5px;">
                <span style="display:inline-block;width:20px;height:20px;border:4px solid {color}; margin-right: 10px; border-radius: 50%;"></span> {schema}
            </div>
        </li>'''
    schema_legend_html += "</ul>"

    # Object Type Legend
    object_legend_html = "<h3>Object Types</h3><ul>"
    for obj_type, color in sorted(object_color_map.items()):
        icon_url = object_icon_map.get(obj_type, get_icon_data_uri("help-circle.svg"))
        object_legend_html += f'''
        <li>
            <div style="display: flex; align-items: center; margin-bottom: 5px;">
                <div style="width: 30px; height: 30px; border-radius: 50%; background-color: {color}; background-image: url({icon_url}); background-size: 70%; background-position: center; background-repeat: no-repeat;"></div>
                <span style="margin-left: 10px;">{obj_type}</span>
            </div>
        </li>
        '''
    object_legend_html += "</ul>"

    legend_html = f'''
    <div style="position: absolute; top: 10px; right: 10px; background: rgba(40, 40, 40, 0.85); color: white; padding: 10px; border-radius: 5px; max-height: 730px; overflow-y: auto; font-family: sans-serif; font-size: 14px; z-index: 1000;">
        {schema_legend_html}
        {object_legend_html}
    </div>
    '''
    return legend_html

def create_dependency_graph_figure(objects: List[Dict[str, Any]], deps: Dict[str, Set[str]], selected_schemas: List[str]):
    # Generates an interactive dependency graph using pyvis.
    net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", notebook=True, cdn_resources='in_line', directed=True) #type: ignore

    # Create a color palette for schemas, ensuring they don't clash with object colors
    schema_colors = {}
    palette = [
        "#8B0000", "#006400", "#00008B", "#4B0082", "#2F4F4F", 
        "#556B2F", "#800000", "#008080", "#B22222", "#DAA520"
    ]
    for i, schema_name in enumerate(sorted(selected_schemas)):
        schema_colors[schema_name] = palette[i % len(palette)]

    # Filter objects based on selected schemas
    selected_nodes = {
        obj["_CANON_FQN"]
        for obj in objects
        if obj.get("schema") in selected_schemas and obj.get("_CANON_FQN")
    }

    # Define color and icon maps
    object_color_map = {
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
    }
    
    object_icon_map = {
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

    # Get the set of object types present in the current selection
    present_object_types = {
        obj.get("object_type", "UNKNOWN")
        for obj in objects
        if obj.get("_CANON_FQN") in selected_nodes
    }

    # Filter the object color and icon maps for the legend
    legend_object_color_map = {
        k: v for k, v in object_color_map.items() if k in present_object_types
    }
    legend_object_icon_map = {
        k: v for k, v in object_icon_map.items() if k in present_object_types
    }

    # Add nodes to the graph
    for obj in objects:
        fqn = obj.get("_CANON_FQN")
        if fqn in selected_nodes:
            obj_type = obj.get("object_type", "UNKNOWN")
            schema_name = obj.get("schema")

            color = object_color_map.get(obj_type, "#90A4AE")
            icon_url = object_icon_map.get(obj_type, get_icon_data_uri("help-circle.svg"))
            
            border_color = schema_colors.get(schema_name, "#FFFFFF")

            node_color = {
                "border": border_color,
                "background": color,
            }
            
            net.add_node(
                fqn, 
                label=obj.get("object_name"), 
                title=f"{obj_type}\n{obj['schema']}.{obj['object_name']}", 
                shape="circularImage", 
                image=icon_url,
                color=node_color, # type: ignore
                borderWidth=3
            )

    # Add edges for the dependencies
    for fqn, dependencies in deps.items():
        if fqn in selected_nodes:
            for dep_fqn in dependencies:
                if dep_fqn in selected_nodes:
                    net.add_edge(source=fqn, to=dep_fqn)

    # Generate the HTML content directly
    try:
        graph_html = net.generate_html()
        legend_html = _generate_legend_html(schema_colors, legend_object_color_map, legend_object_icon_map)
        # Inject legend into the graph HTML
        graph_html = graph_html.replace("</body>", f"{legend_html}</body>")
        return graph_html
    except Exception as e:
        st.error(f"Failed to generate dependency graph: {e}")
        return None

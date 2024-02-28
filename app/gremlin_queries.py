# put in /scripts
# %%
from typing import List
import pandas as pd
import database
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.process.traversal import (
    Barrier,
    Bindings,
    Cardinality,
    Column,
    Direction,
    Operator,
    Order,
    P,
    Pop,
    Scope,
    T,
    WithOptions,
)
from tqdm import tqdm
from database import BulkQueryExecutor
from data_objects import CpG, Factor, Microbe, Disease
import database_connection


# %%
def create_table(processed_data: list):
    # Create a DataFrame with the processed data
    df = pd.DataFrame(processed_data)

    # Adjust the index to start from 1 instead of 0
    df.index = df.index + 1

    # Specify the desired column order
    column_order = ['CpG ID', 'Association', 'Occurrences', 'Direction', 'Beta Baseline', 'M-Value Baseline']

    # Try to reorder the DataFrame columns, catch any KeyError
    try:
        df = df[column_order]
    except KeyError as error:
        missing_columns = [col for col in column_order if col not in df.columns]
        print(f"Missing columns: {missing_columns}")
        raise error

    # Style the DataFrame
    styled_df = df.style.set_table_styles(
        [
            {'selector': 'thead', 'props': [('background-color', 'lightgrey')]},
            {'selector': 'th', 'props': [('font-size', '12pt'), ('text-align', 'center')]},
            {'selector': 'td', 'props': [('text-align', 'center')]},
            {'selector': 'tr:nth-of-type(even)', 'props': [('background-color', '#f2f2f2')]},
            {'selector': 'tr:hover', 'props': [('background-color', '#5cfcff')]}
        ]
    ).set_properties(**{
        'border': '1px solid white',
        'border-collapse': 'collapse',
        'padding': '4px'
    })

    # Convert the styled DataFrame to HTML
    html_table = styled_df.to_html()

    # Return the HTML table string
    return html_table


# %%
def process_cpgs(g, common_cpgs):
    processed_data = []

    for cpg_name, cpg_internal_id in common_cpgs:
        cpg_node = (
            g.V().has('cpg', 'internal ID', cpg_internal_id)
            .valueMap()
            .toList()
        )[0]

        associated_factor = (
            g.V().hasLabel('cpg').has('internal ID', cpg_internal_id)
            .out().hasLabel('factor')
            .values('name')
            .toList()
        )[0]

        processed_entry = {
            'CpG ID': cpg_name,
            'Association': associated_factor,
            'Occurrences': cpg_node.get('occurrences', [None])[0],
            'Direction': cpg_node.get('direction', [None])[0],
            'Beta Baseline': cpg_node.get('beta baseline', [None])[0],
            'M-Value Baseline': cpg_node.get('m-value baseline', [None])[0],
        }
        processed_data.append(processed_entry)

    return processed_data


#  %% Query for CpGs associated with ALL selected factors:
def group_cpgs_by_all_selected_factors(g, factors, cpg_group_name):
    print("RUNNING THE AND FUNCTION!!!")
    cpg_lists = {}
    for factor in factors:
        cpgs_for_factor = (
            g.V()
            .has('factor', 'name', factor)
            .bothE()
            .outV()
            .hasLabel('cpg')
            .project('cpg_name', 'cpg_internal_ID')
            .by('name')
            .by('internal ID')
            .toList()
        )
        print('CPGS FOR FACTOR:', cpgs_for_factor)
        cpg_lists[factor] = {(cpg['cpg_name'], cpg['cpg_internal_ID']) for cpg in cpgs_for_factor}
    print('CPG LISTS DICT:', cpg_lists)

    # Identifying common CpG names
    common_cpg_names = set.intersection(*[set(map(lambda x: x[0], cpgs)) for cpgs in cpg_lists.values()])
    print('NUMBER OF COMMON CPG NAMES:', len(common_cpg_names))
    print('COMMON CPG NAMES:', common_cpg_names)

    # Finding common {'cpg_name': 'cpg_internal_ID'} pairs across all factors
    common_cpgs = {pair for factor_cpgs in cpg_lists.values() for pair in factor_cpgs if pair[0] in common_cpg_names}
    print('NUMBER OF COMMON CPGS:', len(common_cpgs))
    print('**COMMON CPGS:', common_cpgs)

    processed_data = process_cpgs(g, common_cpgs)

    result_table = create_table(processed_data)
    return result_table


# %%
def group_cpgs_by_any_selected_health_factor(
    g: GraphTraversalSource, factors: List[str], cpg_group_name: str
) -> List[dict]:
    print("RUNNING THE OR FUNCTION...")
    associated_cpgs = g.V().hasLabel('cpg').where(
            __.out().hasLabel('factor').has('name', P.within(*factors))
        ).valueMap(True).toList()

    cpg_count = len(associated_cpgs)
    print(cpg_count)
    # print(f"Associated CpGs: {associated_cpgs}")

    # Process the data to create a list of dictionaries for DataFrame construction
    processed_data = []
    for cpg in associated_cpgs:
        cpg_internal_id = cpg.get('internal ID')[0]
        associations_list = (
            g.V().hasLabel('cpg').has('internal ID', cpg_internal_id)
            .out().hasLabel('factor')
            .values('name')
            .toList()
        )
        association_name = next((name for name in associations_list if name in factors), None)

        # Create the entry for the current 'cpg' node including the association name
        processed_entry = {
            'CpG ID': cpg.get('name')[0],
            'Association': association_name,
            'Occurrences': cpg.get('occurrences', [None])[0],
            'Direction': cpg.get('direction', [None])[0],
            'Beta Baseline': cpg.get('beta baseline', [None])[0],
            'M-Value Baseline': cpg.get('m-value baseline', [None])[0],
        }
        processed_data.append(processed_entry)

    result_table = create_table(processed_data)
    return result_table


# %%
def add_cpgs(g: GraphTraversalSource, cpg_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        cpg_df.iterrows(),
        total=cpg_df.shape[0],
        desc="Importing CpGs"
    ):
        cpg_properties = {
            CpG.PropertyKey.NAME: row["CpG"],
            CpG.PropertyKey.INTERNAL_ID: row["Internal ID"],
            CpG.PropertyKey.OCCURENCES: row.get("Occurrences", None),
            CpG.PropertyKey.DIRECTION: row.get("Direction", None),
            CpG.PropertyKey.M_VALUE: row.get("M-Value Baseline", None),
            CpG.PropertyKey.BETA: row.get("Beta Baseline", None)
        }

        query_executor.add_vertex(label=CpG.LABEL, properties=cpg_properties)

    query_executor.force_execute()

    # Retrieve all 'cpg' vertex IDs and map them to their internal IDs
    cpg_node_list = (
        g.V()
        .has_label("cpg")
        .project("id", "internal ID")
        .by(__.id_())
        .by(__.values("internal ID"))
        .to_list()
    )
    cpg_id_dict = {
        cpg_node["internal ID"]: cpg_node["id"] for cpg_node in cpg_node_list
    }

    return cpg_id_dict


# %% Check if cpg has been imported:
def count_nodes_in_db(g: GraphTraversalSource, label: str):
    node_count = g.V().hasLabel(label).count().next()
    return node_count


# %%
def check_node_properties(g: GraphTraversalSource, label: str, property_key: str, property_value: str):
    properties = (
        g.V()
        .hasLabel(label)
        .has(property_key, property_value)
        .project('properties', 'connected_nodes')
        .by(__.valueMap())
        .by(__.both().valueMap().fold())
        .toList()
    )
    return properties


# %%
def add_articles(
    g: GraphTraversalSource,
    article_df: pd.DataFrame,
):
    PROP_KEY_SQL_ID = "_sql_id"
    PROP_KEY_DOI = "doi"

    query_executor = BulkQueryExecutor(g)
    for article_sql_id, article_data in tqdm(
        article_df.iterrows(),
        desc="Importing articles",
        mininterval=1.0,
        total=article_df.shape[0]
    ):
        query_executor.add_vertex(
            label="article",
            properties={
                PROP_KEY_DOI: article_data["DOI"],
                "abstract": article_data["Abstract"],
                "summary": article_data["Summary"],
                PROP_KEY_SQL_ID: article_sql_id,
            },
        )

    query_executor.force_execute()

    # Get a dictionary to map from SQL IDs to graph IDs
    article_node_list = (
        g.V()
        .has(PROP_KEY_SQL_ID)
        .project(PROP_KEY_SQL_ID, "id", PROP_KEY_DOI)
        .by(__.values(PROP_KEY_SQL_ID))
        .by(__.id_())
        .by(__.values(PROP_KEY_DOI))
        .to_list()
    )
    article_id_dict = {
        article_node[PROP_KEY_SQL_ID]: (article_node["id"], article_node[PROP_KEY_DOI])
        for article_node in article_node_list
    }

    # Drop SQL IDs from the graph database
    g.V().properties(PROP_KEY_SQL_ID).drop().iterate()

    return article_id_dict


# %%
def add_factors(g: GraphTraversalSource, factor_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g)
    factor_id_dict = {}

    for factor_sql_id, factor_data in tqdm(
        factor_df.iterrows(),
        desc="Importing factors",
        mininterval=1.0,
        total=factor_df.shape[0],
    ):
        factor_name = str(factor_data["Association"])
        factor_type = str(factor_data["Type"])

        factor_vertex = (
            g.V().has(Factor.LABEL, Factor.PropertyKey.NAME, factor_name)
        )
        if not factor_vertex.hasNext():
            properties = {
                Factor.PropertyKey.NAME: factor_name,
                Factor.PropertyKey.TYPE: factor_type
            }
            query_executor.add_vertex(
                label=Factor.LABEL,
                properties=properties
            )
            print(f"Attempting to add factor vertex: {factor_name}")

            # Force execute to ensure vertex is created
            query_executor.force_execute()

            # Re-query to check if the vertex now exists and retrieve its ID
            factor_vertex = (
                g.V().has(Factor.LABEL, Factor.PropertyKey.NAME, factor_name)
            )
            if factor_vertex.hasNext():
                factor_graph_id = factor_vertex.next().id
                factor_id_dict[factor_sql_id] = factor_graph_id
                print(f"Successfully added and found factor vertex: {factor_name}, ID: {factor_graph_id}")
            else:
                print(f"Failed to find factor vertex immediately after addition: {factor_name}")

        else:
            # Retrieve the ID of the existing vertex
            factor_graph_id = factor_vertex.next().id  # Access as a property
            factor_id_dict[factor_sql_id] = factor_graph_id
            print(f"Found existing factor vertex: {factor_name}, ID: {factor_graph_id}")

    return factor_id_dict


# %%
def add_microbes(g: GraphTraversalSource, microbe_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        microbe_df.iterrows(),
        total=microbe_df.shape[0],
        desc="Ingesting Microbes"
    ):
        microbe_properties = {
            Microbe.PropertyKey.TAXON: index,
            Microbe.PropertyKey.RANK: row["Rank"],
            Microbe.PropertyKey.OCCURENCES: row.get("Occurrences", None),
            Microbe.PropertyKey.DIRECTION: row.get("Direction", None),
            Microbe.PropertyKey.MEAN_ABUNDANCE: row.get("Mean Abundance", None),
            Microbe.PropertyKey.CORRELATION_COEFFICIENT: row.get("Correlation Coefficient", None),
            Microbe.PropertyKey.P_VALUE: row.get("p Value", None),
            Microbe.PropertyKey.Q_VALUE: row.get("q Value", None)
        }

        query_executor.add_vertex(
            label=Microbe.LABEL,
            properties=microbe_properties
        )

    query_executor.force_execute()

    microbe_node_list = (
        g.V()
        .has_label("microbe")
        .project("id", "taxon")
        .by(__.id_())
        .by(__.values("taxon"))
        .to_list()
    )
    microbe_id_dict = {
        microbe_node["taxon"]: microbe_node["id"] for microbe_node in microbe_node_list
    }

    return microbe_id_dict


# %% Import 'disease' nodes
def add_diseases(g: GraphTraversalSource, disease_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    for index, row in tqdm(
        disease_df.iterrows(),
        total=disease_df.shape[0],
        desc="Ingesting Diseases"
    ):
        disease_properties = {
            Disease.PropertyKey.NAME: row["label"],
            Disease.PropertyKey.DOID: row["id"]
        }

        query_executor.add_vertex(
            label=Disease.LABEL,
            properties=disease_properties
        )

    query_executor.force_execute()

    disease_node_list = (
        g.V()
        .has_label("disease")
        .project("id", "disease ontology id")
        .by(__.id_())
        .by(__.values("disease ontology id"))
        .to_list()
    )
    disease_id_dict = {
        disease_node["disease ontology id"]: disease_node["id"] for disease_node in disease_node_list
    }

    return disease_id_dict


# %%
def add_edges_microbes_diseases(g: GraphTraversalSource, microbe_df: pd.DataFrame):
    query_executor = BulkQueryExecutor(g, 100)

    disease_node_list = (
        g.V()
        .has_label("disease")
        .project("id", "disease ontology id")
        .by(__.id_())
        .by(__.values("disease ontology id"))
        .to_list()
    )
    disease_id_dict = {
        disease_node["disease ontology id"]: disease_node["id"] for disease_node in disease_node_list
    }

    microbe_node_list = (
        g.V()
        .has_label("microbe")
        .project("id", "taxon")
        .by(__.id_())
        .by(__.values("taxon"))
        .to_list()
    )
    microbe_id_dict = {
        microbe_node["taxon"]: microbe_node["id"] for microbe_node in microbe_node_list
    }

    # Collecting edge queries
    for _, row_data in tqdm(
        microbe_df.iterrows(),
        total=microbe_df.shape[0],
        desc="Adding microbe-disease edges"
    ):
        # article_graph_id = article_id_dict.get(row_data["Article ID"])
        disease_graph_id = disease_id_dict.get(row_data["DOID"])
        microbe_taxon = row_data["Taxon"]
        microbe_graph_id = microbe_id_dict.get(microbe_taxon)

        g.V(microbe_graph_id).addE("associated with").to(__.V(disease_graph_id)).iterate()

    query_executor.force_execute()

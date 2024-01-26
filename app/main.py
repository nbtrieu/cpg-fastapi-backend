from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from gremlin_queries import group_cpgs_by_all_selected_factors, group_cpgs_by_any_selected_health_factor, add_cpgs, count_nodes_in_db, add_articles, add_factors, check_node_properties, add_microbes, add_diseases, add_edges_microbes_diseases
from models import FactorRequest
import asyncio
import database_connection
import pandas as pd
import requests

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def run_gremlin_in_thread(g, microbe_df):
    # Create a new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Now, run the Gremlin queries
    try:
        add_edges_microbes_diseases(g, microbe_df)
    finally:
        loop.close()


@app.on_event("startup")
async def app_startup():
    # Async operations that should be run when the app starts
    g = database_connection.init_gremlin_client()
    # g = database_connection.get_gremlin_client()
    global node_count
    node_count = await asyncio.to_thread(
        count_nodes_in_db, g, 'disease'
    )
    node_properties = await asyncio.to_thread(
        check_node_properties, g, 'microbe', 'taxon', 'Testing'
    )
    print('NODE COUNT:', node_count)
    print('NODE PROPERTIES:', node_properties)

# @app.on_event("startup")
# async def startup():
#     database_connection.init_gremlin_client()


@app.on_event("shutdown")
async def shutdown():
    database_connection.close_gremlin_client()


@app.get("/")
async def root():
    return {"message": "API is running"}


async def run_gremlin_query(query_func, *args):
    g = database_connection.get_gremlin_client()
    result = await asyncio.to_thread(query_func, g, *args)
    return result


@app.post("/group-cpgs-by-all-selected-factors/", response_class=HTMLResponse)
async def group_cpgs_endpoint_for_AND_function(factor_request: FactorRequest):
    try:
        html_table = await run_gremlin_query(
            group_cpgs_by_all_selected_factors,
            factor_request.factors,
            factor_request.cpg_group_name
        )
        return html_table

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/group-cpgs-by-any-selected-factors/", response_class=HTMLResponse)
async def group_cpgs_endpoint_for_OR_function(factor_request: FactorRequest):
    try:
        html_table = await run_gremlin_query(
            group_cpgs_by_any_selected_health_factor,
            factor_request.factors,
            factor_request.cpg_group_name
        )
        return html_table

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/add-cpgs/", response_class=JSONResponse)
async def add_cpgs_from_csv(file: UploadFile = File(...)):
    try:
        g = database_connection.get_gremlin_client()

        if not file.filename.endswith('.csv'):
            return JSONResponse(status_code=400, content={"message": "Invalid file format"})

        # Convert the uploaded file to a DataFrame
        cpg_df = pd.read_csv(file.file)

        # Pass the DataFrame to the add_cpgs function
        cpg_id_dict = await asyncio.to_thread(add_cpgs, g, cpg_df)

        # Return some indication of success
        return JSONResponse(content={"detail": f"Successfully processed and added {len(cpg_id_dict)} CpGs."})

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@app.get("/download-cpgs-template/")
async def download_template_for_cpgs():
    file_path = "/Users/nicoletrieu/Documents/zymo/cpg-fastapi-backend/app/data/cpgs-template.csv"
    return FileResponse(path=file_path, filename="cpgs-template.csv", media_type='text/csv')


@app.post("/add-articles/", response_class=JSONResponse)
async def add_articles_from_csv(file: UploadFile = File(...)):
    try:
        g = database_connection.get_gremlin_client()

        if not file.filename.endswith('.csv'):
            return JSONResponse(status_code=400, content={"message": "Invalid file format"})

        # Convert the uploaded file to a DataFrame
        article_df = pd.read_csv(file.file)

        # Pass the DataFrame to the add_articles function
        article_id_dict = await asyncio.to_thread(add_articles, g, article_df)

        # Return some indication of success
        return JSONResponse(content={"detail": f"Successfully processed and added {len(article_id_dict)} articles."})

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@app.get("/download-articles-template/")
async def download_template_for_articles():
    file_path = "/Users/nicoletrieu/Documents/zymo/cpg-fastapi-backend/app/data/articles-template.csv"
    return FileResponse(path=file_path, filename="articles-template.csv", media_type='text/csv')


@app.post("/add-factors/", response_class=JSONResponse)
async def add_factors_from_csv(file: UploadFile = File(...)):
    try:
        g = database_connection.get_gremlin_client()

        if not file.filename.endswith('.csv'):
            return JSONResponse(status_code=400, content={"message": "Invalid file format"})

        # Convert the uploaded file to a DataFrame
        factor_df = pd.read_csv(file.file)

        # Pass the DataFrame to the add_factors function
        factor_id_dict = await asyncio.to_thread(add_factors, g, factor_df)

        # Return some indication of success
        return JSONResponse(content={"detail": f"Successfully processed and added {len(factor_id_dict)} factors."})

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@app.get("/download-factors-template/")
async def download_template_for_factors():
    file_path = "/Users/nicoletrieu/Documents/zymo/cpg-fastapi-backend/app/data/factors-template.csv"
    return FileResponse(path=file_path, filename="factors-template.csv", media_type='text/csv')


@app.get("/count-nodes/{label}")
async def count_nodes(label: str):
    g = await database_connection.get_gremlin_client_async()  # assuming this is an async function
    node_count = await count_nodes_in_db(g, label)  # modify check_cpgs_in_db to be async as well
    return node_count


@app.post("/add-microbes/", response_class=JSONResponse)
async def add_microbes_from_csv(file: UploadFile = File(...)):
    try:
        g = database_connection.get_gremlin_client()

        if not file.filename.endswith('.csv'):
            return JSONResponse(status_code=400, content={"message": "Invalid file format"})

        # Convert the uploaded file to a DataFrame
        microbe_df = pd.read_csv(file.file)

        # Pass the DataFrame to the add_microbes function
        microbe_id_dict = await asyncio.to_thread(add_microbes, g, microbe_df)

        # Return some indication of success
        return JSONResponse(content={"detail": f"Successfully processed and added {len(microbe_id_dict)} microbes."})

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@app.get("/download-microbes-template/")
async def download_template_for_microbes():
    s3_url = "https://ds-references.s3.ap-southeast-1.amazonaws.com/bio-annotation/microbes-template.csv"

    try:
        response = requests.get(s3_url, stream=True)
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code

        def iterfile():
            for chunk in response.iter_content(chunk_size=8192):
                yield chunk

        return StreamingResponse(iterfile(), media_type='text/csv', headers={"Content-Disposition": "attachment;filename=microbes-template.csv"})
    except requests.exceptions.HTTPError as http_error:
        raise HTTPException(status_code=response.status_code, detail=str(http_error))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/add-diseases/", response_class=JSONResponse)
async def add_diseases_from_csv(file: UploadFile = File(...)):
    try:
        g = database_connection.get_gremlin_client()

        if not file.filename.endswith('.csv'):
            return JSONResponse(status_code=400, content={"message": "Invalid file format"})

        # Convert the uploaded file to a DataFrame
        disease_df = pd.read_csv(file.file)

        # Pass the DataFrame to the add_diseases function
        disease_id_dict = await asyncio.to_thread(add_diseases, g, disease_df)

        # Return some indication of success
        return JSONResponse(content={"detail": f"Successfully processed and added {len(disease_id_dict)} diseases."})

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})


@app.post("/connect-microbes-to-diseases/")
async def connect_microbes_to_diseases():
    try:
        # Define the file path
        file_path = '/Users/nicoletrieu/Documents/zymo/cpg-fastapi-backend/app/data/microbes.csv'

        # Read the content of the uploaded CSV file into a pandas DataFrame
        microbe_df = pd.read_csv(file_path)

        # Creating a Gremlin traversal source (g) - Replace with your actual connection code
        g = database_connection.get_gremlin_client()

        # Call your function to add edges
        await asyncio.to_thread(run_gremlin_in_thread, g, microbe_df)

        return {"message": "Successfully added edges between microbes and diseases"}

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"detail": str(e)})

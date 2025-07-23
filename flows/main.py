from prefect import serve
from domain.elections.refresh_election_data import refresh_election_data_deployment

if __name__ == "__main__":
    serve(
        refresh_election_data_deployment,
        pause_on_shutdown=False
    )

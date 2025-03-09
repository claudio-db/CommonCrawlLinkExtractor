#!/bin/bash

function show_help {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start      Start all containers"
    echo "  stop       Stop all containers"
    echo "  status     Show container status"
    echo "  logs       Show logs (use -f for follow, add service name to see specific logs)"
    echo "  airflow    Open Airflow web interface in the default browser"
    echo "  trigger    Trigger a DAG run (requires DAG ID parameter)"
    echo "  rebuild    Rebuild and restart all containers"
    echo "  clean      Stop containers and remove volumes (WARNING: deletes all data)"
    echo "  help       Show this help message"
}

case "$1" in
    start)
        echo "Starting all containers..."
        docker-compose up -d
        echo "Creating necessary directories..."
        mkdir -p dags plugins logs configs output dist
        echo "Environment started. Airflow available at http://localhost:8080"
        ;;
    
    stop)
        echo "Stopping all containers..."
        docker-compose stop
        ;;
    
    status)
        echo "Container status:"
        docker-compose ps
        ;;
    
    logs)
        shift
        docker-compose logs $@
        ;;
    
    airflow)
        # Open Airflow UI in the browser
        if command -v xdg-open &> /dev/null; then
            xdg-open http://localhost:8080
        elif command -v open &> /dev/null; then
            open http://localhost:8080
        else
            echo "Airflow UI available at: http://localhost:8080"
        fi
        ;;
    
    trigger)
        if [ -z "$2" ]; then
            echo "Error: DAG ID required."
            echo "Usage: $0 trigger DAG_ID [--conf '{\"key\":\"value\"}']"
            exit 1
        fi
        
        DAG_ID="$2"
        shift 2
        
        echo "Triggering DAG: $DAG_ID"
        docker-compose exec airflow-webserver airflow dags trigger $DAG_ID $@
        ;;
    
    rebuild)
        echo "Rebuilding environment..."
        docker-compose down
        docker-compose build
        docker-compose up -d
        echo "Environment rebuilt and started."
        ;;
    
    clean)
        echo "WARNING: This will remove all containers and volumes, deleting all data."
        read -p "Are you sure you want to continue? (y/N) " confirm
        if [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]]; then
            echo "Stopping containers and removing volumes..."
            docker-compose down -v
            echo "Cleanup complete."
        else
            echo "Operation cancelled."
        fi
        ;;
    
    help|*)
        show_help
        ;;
esac

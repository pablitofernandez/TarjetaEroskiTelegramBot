# Telegram Eroski Credit Card Excel Processor & Notifier

This project provides an automated workflow to process Eroski credit card exported excel files received via Telegram, store the transactions in a database, send email notifications for new movements, and allow querying recent transactions directly from the Telegram bot.

## Features

*   **Telegram Bot Listener:** A dedicated bot listens for Excel files (`.xlsx`, `.xls`) sent by an authorized user.
*   **API-based Processing:** Received Excel files are sent directly to a processing API.
*   **Data Persistence:** Transactions are parsed and stored in a persistent SQLite database, avoiding duplicates.
*   **Email Notifications:** Sends formatted HTML email notifications summarizing newly detected transactions.
*   **Telegram Feedback:** Provides immediate feedback to the user in Telegram about the processing status (success, partial errors, failures).
*   **Transaction Query:** Allows the authorized user to query the last N transactions using a `/last N` command in Telegram.
*   **Dockerized:** Both the Telegram Bot and the Bank Processor run as separate services orchestrated by Docker Compose for easy deployment and management.

## Architecture

1.  **`telegram-bot` Service:**
    *   Runs a Python Telegram bot using `python-telegram-bot`.
    *   Listens for messages from the `ALLOWED_USER_ID`.
    *   When an Excel file is received, it downloads it to memory.
    *   Sends the file content via HTTP POST to the `bank-processor` API's `/api/process_excel` endpoint.
    *   Receives a JSON response from the API with the processing result.
    *   Sends a formatted feedback message back to the Telegram user.
    *   Handles the `/last N` command by querying the `bank-processor` API's `/api/last_transactions` endpoint.
2.  **`bank-processor` Service:**
    *   Runs a Flask API server (`api_server.py`).
    *   Exposes endpoints:
        *   `POST /api/process_excel`: Receives the Excel file, saves it temporarily, calls the processing logic (`process_bank_excel.py`), and returns a JSON result. Sends email notifications if new transactions are found.
        *   `GET /api/last_transactions`: Queries the SQLite database (`transactions.db`) and returns the last N transactions as JSON.
        *   `GET /health`: A simple health check endpoint.
    *   Stores transaction data in an SQLite database located in a persistent Docker volume (`/app/state`).

## Prerequisites

*   Docker ([https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/))
*   Docker Compose ([https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/))
*   A Telegram Bot Token ([Create one via @BotFather](https://t.me/BotFather))
*   Your Telegram User ID ([Find it using a bot like @userinfobot](https://t.me/userinfobot))
*   SMTP Server details (Server, Port, Login, Password - **Use an App Password if using Gmail/Outlook**) for sending email notifications.

## Setup & Running

1.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url>
    cd <repository-directory>
    ```

2.  **Create Environment File:**
    *   Copy the example environment file:
        ```bash
        cp .env.example .env
        ```
    *   **Edit the `.env` file** and fill in your actual credentials and configuration:
        *   `SMTP_SERVER`, `SMTP_PORT`, `EMAIL_SENDER`, `EMAIL_LOGIN`, `EMAIL_PASSWORD`, `EMAIL_RECEIVER`
        *   `BOT_TOKEN`
        *   `ALLOWED_USER_ID`
        *   Adjust `COL_*_EXCEL` variables if your Excel column names are different.
        *   Set your `TZ` (Timezone, e.g., `Europe/Madrid`, `America/New_York`).

3.  **Build and Run with Docker Compose:**
    ```bash
    docker-compose build
    docker-compose up -d
    ```
    This will build the Docker images for both services and start them in detached mode.

4.  **Interact with the Bot:**
    *   Find your bot on Telegram (the one associated with `BOT_TOKEN`).
    *   Send it an Excel file (`.xlsx` or `.xls`) containing your bank transactions. You should receive feedback messages.
    *   Use the `/last` command (e.g., `/last 10`) to view recent transactions.

5.  **Check Logs (Optional):**
    ```bash
    # View logs for both services
    docker-compose logs -f

    # View logs for a specific service
    docker-compose logs -f telegram-bot
    docker-compose logs -f bank-processor
    ```

6.  **Stopping the Services:**
    ```bash
    docker-compose down
    ```
    Use `docker-compose down -v` if you also want to remove the persistent data volume (including the database).

## Configuration Notes

*   **Excel Columns:** Make sure the `COL_DATE_EXCEL`, `COL_DESC_EXCEL`, `COL_AMOUNT_EXCEL` variables in `.env` **exactly** match the column headers in your Excel file. Set `COL_BANK_ID_EXCEL` if your bank provides a unique transaction ID column.
*   **Email Security:** For services like Gmail or Outlook, you will likely need to generate an "App Password" to use in the `EMAIL_PASSWORD` field instead of your regular account password.
*   **Database Location:** The SQLite database (`transactions.db` by default) is stored in the `data/state` directory relative to your `docker-compose.yml` file on the host machine (ensure this path exists or Docker can create it). **Do not commit the `data/` directory to Git.**

## Contributing

Feel free to open issues or pull requests if you have suggestions or improvements!
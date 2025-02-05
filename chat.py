import mysql.connector
from typing import Tuple, Optional
from mysql.connector.cursor import MySQLCursor
from config.config_heatwave import DB_CONFIG  # Import the MySQL configuration


def connect_to_mysql() -> mysql.connector.MySQLConnection:
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(**DB_CONFIG)


def load_llm(cursor: MySQLCursor, llm_options: Tuple[str, ...]) -> None:
    """Load language models into HeatWave."""
    sql_load_llm = 'sys.ML_MODEL_LOAD'
    for llm in llm_options:
        args = (llm, 'NULL')
        cursor.callproc(sql_load_llm, args)
        print(f"LLM Loaded: {llm}")
    print("All LLMs loaded successfully.")


def select_llm(llm_options: Tuple[str, ...]) -> str:
    """
    Prompt the user to select an LLM from the provided options.
    
    Supports up to 4 choices.
    """
    option_map = {str(i + 1): llm for i, llm in enumerate(llm_options)}

    while True:
        # Dynamically build the prompt based on available options
        prompt = "Choose your LLM:\n"
        for i, llm in enumerate(llm_options):
            prompt += f"{i + 1}-({llm})\n"
        prompt += "Enter your choice: "

        choice = input(prompt)

        # Validate user input
        if choice in option_map:
            return option_map[choice]

        print(f"Invalid choice. Please select a number between 1 and {len(llm_options)}.")


def chat(cursor: MySQLCursor, llm: str) -> Optional[str]:
    """Facilitate user chat with HeatWave."""
    question = input("Ask HeatWave?> ")
    response, chat_info = hw_chat(cursor, question, llm)
    print(f"Response: {response}")
    print("-" * 30)
    return chat_info


def hw_chat(cursor: MySQLCursor, user_query: str, llm: str) -> Tuple[str, Optional[str]]:
    """Send a user query to HeatWave and return the response."""
    sp_hw_chat = 'sys.HEATWAVE_CHAT'
    cursor.callproc(sp_hw_chat, (user_query,))
    response = ""
    for result in cursor.stored_results():
        response = result.fetchone()[0]

    chat_info = get_chat_options(cursor)
    return response, chat_info


def get_chat_options(cursor: MySQLCursor) -> Optional[str]:
    """Retrieve the session variable 'chat_options'."""
    cursor.execute("SELECT @chat_options")
    chat_options = cursor.fetchone()[0]
    return chat_options


def set_chat_options(cursor: MySQLCursor, llm: str) -> None:
    """Initialize or update the session variable 'chat_options'."""
    chat_options = get_chat_options(cursor)
    if not chat_options:
        # Initialize @chat_options if not set
        options = f'{{"model_options": {{"model_id": "{llm}"}}}}'
        sql = f"SET @chat_options = '{options}'"
    else:
        # Update @chat_options if already exists
        sql = f"SET @chat_options = JSON_SET(@chat_options, '$.model_options.model_id', '{llm}')"
    cursor.execute(sql)
    print(f"Using model: {llm}")
    print("-" * 40)


def main() -> None:
    """Main function to run the LLM interaction."""
    try:
        with connect_to_mysql() as connection:
            with connection.cursor() as cursor:
                # Define available LLM options
                llm_options = ("llama3-8b-instruct-v1", "mistral-7b-instruct-v1", "cohere.command-r-plus-08-2024", "meta.llama-3.1-70b-instruct")

                # Load LLMs
                load_llm(cursor, llm_options)

                # Prompt user to select an LLM
                selected_llm = select_llm(llm_options)

                # Set chat options for the selected LLM
                set_chat_options(cursor, selected_llm)

                # Begin chat loop
                while True:
                    chat(cursor, selected_llm)
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except KeyboardInterrupt:
        print("\nExiting the application.")
    finally:
        print("Goodbye!")


if __name__ == "__main__":
    main()

from main_app.bonus.export_cases_by_numbers import export_cases_by_numbers
from main_app.paths import DB_PATH, BONUS__OUTPUT_CSV, BONUS__INPUT_CSV

if __name__ == '__main__':
    export_cases_by_numbers(
        db_path=DB_PATH,
        input_cases_csv=BONUS__INPUT_CSV,
        output_csv=BONUS__OUTPUT_CSV,
    )

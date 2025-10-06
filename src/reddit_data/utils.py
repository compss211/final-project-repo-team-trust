def calculate_average(numbers):
    # Calculate the average of a list of numbers.
    if not numbers:
        return 0
    return sum(numbers) / len(numbers)

def format_currency(amount, currency="USD"):
    # Format a number as currency.
    symbols = {"USD": "$", "EUR": "€", "GBP": "£"}
    symbol = symbols.get(currency, "$")
    return f"{symbol}{amount:.2f}"
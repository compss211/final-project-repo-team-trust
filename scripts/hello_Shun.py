"""
A simple hello world script by [Shun Moriguchi]
"""
name = "[Shun Moriguchi]"
print(f"Hello from {name}!")
print("This is my first script in our group project.")

# Add a simple calculation
favorite_number = 7  # Change this to your actual favorite number
result = favorite_number * 2
print(f"My favorite number times 2 is: {result}")

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
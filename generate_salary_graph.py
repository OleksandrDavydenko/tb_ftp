import matplotlib.pyplot as plt
import pandas as pd

def generate_salary_graph():
    # Приклад даних для нарахування ЗП
    data = {
        'Місяць': ['Січень', 'Лютий', 'Березень', 'Квітень'],
        'Сума': [1000, 1500, 1200, 1700]
    }
    df = pd.DataFrame(data)

    # Генерація графіка
    plt.figure(figsize=(8, 4))
    plt.bar(df['Місяць'], df['Сума'], color='red')
    plt.title('Нарахування ЗП за місяцями')
    plt.xlabel('Місяць')
    plt.ylabel('Сума')
    plt.grid(axis='y')

    # Збереження графіка
    plt.savefig('salary_accruals.png')
    plt.close()

import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_pie_chart(debt_data, user_name, temp_dir):
    # Перетворюємо список словників у DataFrame
    debtors_df = pd.DataFrame(debt_data)

    if debtors_df.empty:
        print(f"Дані відсутні для {user_name} для побудови секторної діаграми.")
        return None

    # Перевіряємо правильність стовпців
    if '[Client]' not in debtors_df.columns or '[Sum_$]' not in debtors_df.columns:
        print(f"Відсутні необхідні стовпці у даних для {user_name}.")
        return None

    # Отримання даних для конкретного менеджера
    user_debts = debtors_df[['[Client]', '[Sum_$]']]

    if user_debts.empty:
        print(f"Немає даних для побудови діаграми для {user_name}.")
        return None

    # Функція для форматування підписів
    def autopct_format(pct, all_values):
        absolute = int(round(pct / 100. * sum(all_values)))
        return f'{pct:.1f}%\n(${absolute})'

    # Побудова секторної діаграми
    plt.figure(figsize=(8, 8))
    plt.pie(
        user_debts['[Sum_$]'],
        labels=user_debts['[Client]'],
        autopct=lambda pct: autopct_format(pct, user_debts['[Sum_$]']),
        startangle=140
    )
    plt.title(f'Дебіторська заборгованість для {user_name}')
    plt.tight_layout()

    # Збереження графіка в тимчасовій папці
    file_path = os.path.join(temp_dir, 'debt_pie_chart.png')
    plt.savefig(file_path)
    plt.close()

    return file_path

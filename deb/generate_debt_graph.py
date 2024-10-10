import pandas as pd
import matplotlib.pyplot as plt
import os

def generate_debt_graph(debt_data, user_name, temp_dir):
    # Перетворюємо список словників у DataFrame
    debtors_df = pd.DataFrame(debt_data)

    if debtors_df.empty:
        print(f"Дані відсутні для {user_name} для побудови графіка.")
        return None

    # Перевіряємо правильність стовпців
    if '[Client]' not in debtors_df.columns or '[Sum_$]' not in debtors_df.columns:
        print(f"Відсутні необхідні стовпці у даних для {user_name}.")
        return None

    # Отримання даних для конкретного менеджера
    user_debts = debtors_df[['[Client]', '[Sum_$]']]

    if user_debts.empty:
        print(f"Немає даних для побудови графіка для {user_name}.")
        return None

    # Побудова графіка
    plt.figure(figsize=(10, 6))
    plt.bar(user_debts['[Client]'], user_debts['[Sum_$]'], color='skyblue')
    plt.xlabel('Контрагент')
    plt.ylabel('Сума (USD)')
    plt.title(f'Дебіторська заборгованість для {user_name}')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Збереження графіка в тимчасовій папці
    file_path = os.path.join(temp_dir, 'debt_graph.png')
    plt.savefig(file_path)
    plt.close()

    return file_path

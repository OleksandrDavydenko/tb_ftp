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

    # Групування даних за клієнтами та підсумовування сум
    aggregated_data = debtors_df.groupby('[Client]', as_index=False)['[Sum_$]'].sum()

    if aggregated_data.empty:
        print(f"Немає даних для побудови графіка для {user_name}.")
        return None

    # Побудова графіка
    plt.figure(figsize=(10, 6))
    bars = plt.bar(aggregated_data['[Client]'], aggregated_data['[Sum_$]'], color='skyblue')
    plt.xlabel('Контрагент')
    plt.ylabel('Сума (USD)')
    plt.title(f'Дебіторська заборгованість для {user_name}')
    plt.xticks(rotation=45, ha='right')

    # Додавання значень над стовпцями
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, f'{height:.2f}', ha='center', va='bottom')

    plt.tight_layout()

    # Збереження графіка в тимчасовій папці
    file_path = os.path.join(temp_dir, 'debt_graph.png')
    plt.savefig(file_path)
    plt.close()

    return file_path
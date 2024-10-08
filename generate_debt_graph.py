import matplotlib.pyplot as plt

def generate_debt_graph(debtors_df, user_name):
    # Отримання даних для конкретного менеджера
    user_debts = debtors_df[(debtors_df['Manager'] == user_name) & (debtors_df['Inform'] != 1)]

    if user_debts.empty:
        print("Дані відсутні для побудови графіка.")
        return

    # Побудова графіка
    plt.figure(figsize=(10, 6))
    plt.bar(user_debts['Client'], user_debts['Sum_$'], color='skyblue')
    plt.xlabel('Контрагент')
    plt.ylabel('Сума (USD)')
    plt.title(f'Дебіторська заборгованість для {user_name}')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Збереження графіка
    plt.savefig('debt_graph.png')
    plt.close()

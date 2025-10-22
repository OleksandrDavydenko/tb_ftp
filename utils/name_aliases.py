# utils/name_aliases.py
NAME_ALIAS = {
    "Боковий Максим": "Maxim Boyko",
    "Зотова Христина": "Кристи Андреева",
    "Антонов Артемій": "Andrii Himanov",
    "Ткачов Олександр": "Kovalenko Stanislav",
    "Жученко Олександр": "Aleksandr Lafayett",
    "Вовк Людмила": "Liudmyla Makievska",
    "Панько Олександр": "Alex Glebov",
    "Ковальчук Олександра": "Оlexa Kovach",
    "Кубрак Ольга": "Lara Green",
    "Ястрєбов Олександр": "Алекс Мазур",
    "Окулов Костянтин": "Виноградов Володимир",
}

def display_name(src: str) -> str:
    """Повертає ‘красиве’ ім’я лише для відомих у словнику, інакше — як є."""
    return NAME_ALIAS.get(src, src)

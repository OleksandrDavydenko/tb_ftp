# utils/name_aliases.py
NAME_ALIAS = {
    "Боковий Максим": "Maxim Boyko",
    "Кристина Зотова": "Кристи Андреева",
    "Антонов Артемій": "Andrii Himanov",
    "Олександр Ткачов": "Kovalenko Stanislav",
    "Жученко Олександр": "Aleksandr Lafayett",
    "Людмила Вовк": "Liudmyla Makievska",
    "Панько Олександр": "Alex Glebov",
    "Ковальчук Олександра": "Оlexa Kovach",
    "Кубрак Ольга": "Lara Green",
    "Ястребов Олександр": "Алекс Мазур",
}

def display_name(src: str) -> str:
    """Повертає ‘красиве’ ім’я лише для відомих у словнику, інакше — як є."""
    return NAME_ALIAS.get(src, src)

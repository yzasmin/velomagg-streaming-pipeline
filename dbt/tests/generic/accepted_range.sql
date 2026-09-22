{# Test générique : la colonne reste dans [min_value, max_value]. Chaque borne est optionnelle.
   Écrit ici pour éviter une dépendance à dbt_utils (pas de téléchargement au démarrage). #}
{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select *
from {{ model }}
where {{ column_name }} is null
{% if min_value is not none %} or {{ column_name }} < {{ min_value }}{% endif %}
{% if max_value is not none %} or {{ column_name }} > {{ max_value }}{% endif %}
{% endtest %}

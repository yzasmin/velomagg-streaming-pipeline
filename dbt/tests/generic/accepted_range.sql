{# Test générique : la colonne reste dans [min_value, max_value]. Chaque borne est optionnelle.
   Les valeurs nulles sont ignorées (c'est le rôle du test not_null), sinon une colonne nulle
   par construction, comme la latence d'un relevé rejoué, ferait échouer le test.
   Écrit ici pour éviter une dépendance à dbt_utils (pas de téléchargement au démarrage). #}
{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    false
    {% if min_value is not none %} or {{ column_name }} < {{ min_value }}{% endif %}
    {% if max_value is not none %} or {{ column_name }} > {{ max_value }}{% endif %}
  )
{% endtest %}

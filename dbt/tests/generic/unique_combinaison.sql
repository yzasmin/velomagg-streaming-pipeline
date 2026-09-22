{# Test générique de modèle : la combinaison de colonnes est unique (clé de déduplication). #}
{% test unique_combinaison(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

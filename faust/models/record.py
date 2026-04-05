"""Record - Dictionary Model."""
from datetime import datetime
from decimal import Decimal
from itertools import chain
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    MutableMapping,
    Set,
    Tuple,
    Type,
    cast,
)

from mode.utils.compat import OrderedDict
from mode.utils.objects import (
    annotations,
    is_optional,
    remove_optional,
)
from mode.utils.text import pluralize

from faust.types.models import (
    CoercionMapping,
    FieldDescriptorT,
    FieldMap,
    IsInstanceArgT,
    ModelOptions,
    ModelT,
)
from faust.utils import codegen

from .base import Model
from .fields import FieldDescriptor, field_for_type
from .tags import Tag

__all__ = ['Record']

DATE_TYPES: IsInstanceArgT = (datetime,)
DECIMAL_TYPES: IsInstanceArgT = (Decimal,)

ALIAS_FIELD_TYPES = {
    dict: Dict,
    tuple: Tuple,
    list: List,
    set: Set,
    frozenset: FrozenSet,
}

E_NON_DEFAULT_FOLLOWS_DEFAULT = '''
Non-default {cls_name} field {field_name} cannot
follow default {fields} {default_names}
'''

_ReconFun = Callable[..., Any]


class Record(Model, abstract=True):  # type: ignore
    """Describes a model type that is a record (Mapping).

    Examples:
        >>> class LogEvent(Record, serializer='json'):
        ...     severity: str
        ...     message: str
        ...     timestamp: float
        ...     optional_field: str = 'default value'

        >>> event = LogEvent(
        ...     severity='error',
        ...     message='Broken pact',
        ...     timestamp=666.0,
        ... )

        >>> event.severity
        'error'

        >>> serialized = event.dumps()
        '{"severity": "error", "message": "Broken pact", "timestamp": 666.0}'

        >>> restored = LogEvent.loads(serialized)
        <LogEvent: severity='error', message='Broken pact', timestamp=666.0>

        >>> # You can also subclass a Record to create a new record
        >>> # with additional fields
        >>> class RemoteLogEvent(LogEvent):
        ...     url: str

        >>> # You can also refer to record fields and pass them around:
        >>> LogEvent.severity
        >>> <FieldDescriptor: LogEvent.severity (str)>
    """

    def __init_subclass__(cls,
                          serializer: str = None,
                          namespace: str = None,
                          include_metadata: bool = None,
                          isodates: bool = None,
                          abstract: bool = False,
                          allow_blessed_key: bool = None,
                          decimals: bool = None,
                          coerce: bool = None,
                          coercions: CoercionMapping = None,
                          polymorphic_fields: bool = None,
                          validation: bool = None,
                          date_parser: Callable[[Any], datetime] = None,
                          lazy_creation: bool = False,
                          **kwargs: Any) -> None:
        # XXX mypy 0.750 requires this to be defined on the class,
        # and do not recognize the parent class signature.

        super().__init_subclass__(
            serializer=serializer,
            namespace=namespace,
            include_metadata=include_metadata,
            isodates=isodates,
            abstract=abstract,
            allow_blessed_key=allow_blessed_key,
            decimals=decimals,
            coerce=coerce,
            coercions=coercions,
            polymorphic_fields=polymorphic_fields,
            validation=validation,
            date_parser=date_parser,
            lazy_creation=lazy_creation,
            **kwargs)

    @classmethod
    def _contribute_to_options(cls, options: ModelOptions) -> None:
        # Find attributes and their types, and create indexes for these.
        # This only happens once when the class is created, so Faust
        # models are fast at runtime.

        pass

    @classmethod
    def _contribute_methods(cls) -> None:
        pass

    @classmethod
    def _contribute_field_descriptors(
            cls,
            target: Type,
            options: ModelOptions,
            parent: FieldDescriptorT = None) -> FieldMap:
        fields = options.fields
        defaults = options.defaults
        date_parser = options.date_parser
        coerce = options.coerce
        index = {}

        secret_fields = set()
        sensitive_fields = set()
        personal_fields = set()
        tagged_fields = set()

        def add_to_tagged_indices(field: str, tag: Type[Tag]) -> None:
            if tag.is_secret:
                options.has_secret_fields = True
                secret_fields.add(field)
            if tag.is_sensitive:
                options.has_sensitive_fields = True
                sensitive_fields.add(field)
            if tag.is_personal:
                options.has_personal_fields = True
                personal_fields.add(field)
            options.has_tagged_fields = True
            tagged_fields.add(field)

        def add_related_to_tagged_indices(field: str,
                                          related_model: Type = None) -> None:
            if related_model is None:
                return
            try:
                related_options = related_model._options
            except AttributeError:
                return
            if related_options.has_secret_fields:
                options.has_secret_fields = True
                secret_fields.add(field)
            if related_options.has_sensitive_fields:
                options.has_sensitive_fields = True
                sensitive_fields.add(field)
            if related_options.has_personal_fields:
                options.has_personal_fields = True
                personal_fields.add(field)
            if related_options.has_tagged_fields:
                options.has_tagged_fields = True
                tagged_fields.add(field)

        for field, typ in fields.items():
            try:
                default, needed = defaults[field], False
            except KeyError:
                default, needed = None, True
            descr = getattr(target, field, None)
            if is_optional(typ):
                target_type = remove_optional(typ)
            else:
                target_type = typ
            if descr is None or not isinstance(descr, FieldDescriptorT):
                DescriptorType, tag = field_for_type(target_type)
                if tag:
                    add_to_tagged_indices(field, tag)
                descr = DescriptorType(
                    field=field,
                    type=typ,
                    model=cls,
                    required=needed,
                    default=default,
                    parent=parent,
                    coerce=coerce,
                    model_coercions=options.coercions,
                    date_parser=date_parser,
                    tag=tag,
                )
            else:
                descr = descr.clone(
                    field=field,
                    type=typ,
                    model=cls,
                    required=needed,
                    default=default,
                    parent=parent,
                    coerce=coerce,
                    model_coercions=options.coercions,
                )

            descr.on_model_attached()

            for related_model in descr.related_models:
                add_related_to_tagged_indices(field, related_model)
            setattr(target, field, descr)
            index[field] = descr

        options.secret_fields = frozenset(secret_fields)
        options.sensitive_fields = frozenset(sensitive_fields)
        options.personal_fields = frozenset(personal_fields)
        options.tagged_fields = frozenset(tagged_fields)
        return index

    @classmethod
    def from_data(cls, data: Mapping, *,
                  preferred_type: Type[ModelT] = None) -> 'Record':
        """Create model object from Python dictionary."""
        # check for blessed key to see if another model should be used.
        if hasattr(data, '__is_model__'):
            return cast(Record, data)
        else:
            self_cls = cls._maybe_namespace(
                data, preferred_type=preferred_type)
        cls._input_translate_fields(data)
        return (self_cls or cls)(**data, __strict__=False)

    def __init__(self, *args: Any,
                 __strict__: bool = True,
                 __faust: Any = None,
                 **kwargs: Any) -> None:  # pragma: no cover
        ...  # overridden by _BUILD_init

    @classmethod
    def _BUILD_input_translate_fields(cls) -> Callable[[MutableMapping], None]:
        pass

    @classmethod
    def _BUILD_init(cls) -> Callable[[], None]:
        # generate init function that set field values from arguments,
        # and that load data from serialized form.
        #
        #
        # The general template that we will be generating is
        #
        #    def __outer__(Model):   # create __init__ closure
        #       __defaults__ = Model._options.defaults
        #       __descr__ = Model._options.descriptors
        #       {% for field in fields_with_defaults %}
        #       _default_{{ field }}_ = __defaults__["{{ field }}"]
        #       {% endfor %}
        #       {% for field in fields_with_init %}
        #       _init_{{ field }}_ = __descr__["{{ field }}"].to_python
        #
        #       def __init__(self, {{ sig }}, *, __strict__=True, **kwargs):
        #          self.__evaluated_fields__ = set()
        #          if __strict__:  # creating model from Python
        #              {% for field in fields %}
        #              self.{{ field }} = {{ field }}
        #              {% endfor %}
        #              if kwargs:
        #                 # raise error for additional arguments
        #          else:
        #              {% for field in fields %}
        #              {% if OPTIONAL_FIELD(field) %}
        #              if {{ field }} is not None:
        #                  self.{{ field }} = _init_{{ field }}({{ field }})
        #              else:
        #                  self.{{ field }} = _default_{{ field }}
        #              {% else %}
        #                  self.{{ field }} = _init_{{ field }}({{ field }}
        #              # any additional kwargs are added as fields
        #              # when loading from serialized data.
        #              self.__dict__.update(kwargs)
        #         self.__post_init__()
        #     return __init__
        #
        pass

    @classmethod
    def _BUILD_hash(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_eq(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_ne(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_gt(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_ge(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_lt(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_le(cls) -> Callable[[], None]:
        pass

    @classmethod
    def _BUILD_asdict(cls) -> Callable[..., Dict[str, Any]]:
        pass

    def _prepare_dict(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @classmethod
    def _BUILD_asdict_field(cls, name: str, field: FieldDescriptorT) -> str:
        pass

    def _derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        pass

    def to_representation(self) -> Mapping[str, Any]:
        """Convert model to its Python generic counterpart.

        Records will be converted to dictionary.
        """
        # Convert known fields to mapping of ``{field: value}``.
        payload = self.asdict()
        options = self._options
        if options.include_metadata:
            payload['__faust'] = {'ns': options.namespace}
        return payload

    def asdict(self) -> Dict[str, Any]:  # pragma: no cover
        """Convert record to Python dictionary."""
        ...  # generated by _BUILD_asdict
    # Used to disallow overriding this method
    asdict.faust_generated = True  # type: ignore

    def _humanize(self) -> str:
        # we try to preserve the order of fields specified in the class,
        # so doing {**self._options.defaults, **self.__dict__} does not work.
        pass

    def __json__(self) -> Any:
        return self.to_representation()

    def __eq__(self, other: Any) -> bool:  # pragma: no cover
        # implemented by BUILD_eq
        return NotImplemented

    def __ne__(self, other: Any) -> bool:  # pragma: no cover
        # implemented by BUILD_ne
        return NotImplemented

    def __lt__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_lt
        return NotImplemented

    def __le__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_le
        return NotImplemented

    def __gt__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_gt
        return NotImplemented

    def __ge__(self, other: 'Record') -> bool:  # pragma: no cover
        # implemented by BUILD_ge
        return NotImplemented


def _kvrepr(d: Mapping[str, Any], *, sep: str = ', ') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    pass

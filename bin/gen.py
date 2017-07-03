# -*- coding:utf-8 -*-
import itertools
from inflection import camelize, singularize, pluralize
from collections import OrderedDict
from sqlalchemy.ext import automap
from sqlalchemy import create_engine
from sqlalchemy.inspection import inspect
from dictknife import loading
import logging
logger = logging.getLogger(__name__)


def _merge(*xss):
    return [xs for xs in xss if xs]


class Collector:
    def __init__(self, resolver):
        self.resolver = resolver

    def collect(self, classes):
        d = OrderedDict()
        for c in classes:
            mapper = inspect(c)
            d[classname_for_table(mapper.local_table.fullname)] = self._collect_from_mapper(mapper)
        return d

    def _collect_from_mapper(self, m):
        d = OrderedDict()
        d["table"] = m.local_table.fullname
        for prop in sorted(m.iterate_properties, key=lambda x: x.key):
            if hasattr(prop, "direction"):
                if "relationship" not in d:
                    d["relationship"] = OrderedDict()
                d["relationship"][prop.key] = OrderedDict(
                    [
                        ("table", prop.target.fullname),
                        ("clsname", classname_for_table(prop.target.fullname)),
                        ("direction", prop.direction.name),
                        ("uselist", prop.uselist),
                        (
                            "relation", _merge(
                                self._collect_primary_relation(prop),
                                self._collect_secondary_relation(prop),
                            )
                        ),
                    ]
                )
            else:
                if "column" not in d:
                    d["column"] = OrderedDict()
                assert len(prop.columns) == 1, "multi keys are not supported"
                c = prop.columns[0]
                d["column"][prop.key] = OrderedDict(
                    [
                        ("type", self.resolver.resolve_type(c)),
                        ("nullable", c.nullable),
                    ]
                )
        return d

    def _collect_primary_relation(self, prop):
        pairs = prop.synchronize_pairs
        if not pairs:
            return []
        assert len(pairs) == 1, "multi keys are not supported"
        if prop.parent.local_table == pairs[0][0].table:
            from_, to = pairs[0][0], pairs[0][1]
        else:
            to, from_ = pairs[0][0], pairs[0][1]
        return OrderedDict(
            [
                ("from", "{}.{}".format(from_.table.fullname, from_.name)),
                ("to", "{}.{}".format(to.table.fullname, to.name)),
            ]
        )

    def _collect_secondary_relation(self, prop):
        pairs = prop.secondary_synchronize_pairs
        if not pairs:
            return []
        assert len(pairs) == 1, "multi keys are not supported"
        if prop.target == pairs[0][1].table:
            from_, to = pairs[0][0], pairs[0][1]
        else:
            to, from_ = pairs[0][0], pairs[0][1]
        return OrderedDict(
            [
                ("from", "{}.{}".format(from_.table.fullname, from_.name)),
                ("to", "{}.{}".format(to.table.fullname, to.name)),
            ]
        )


class Resolver:
    mapping = {int: "Integer", str: "String"}

    def __init__(self, mapping=None):
        self.mapping = mapping or self.__class__.mapping

    def resolve_type(self, c):
        if c.primary_key:
            return "ID"
        typ = self.mapping.get(c.type.python_type)
        if typ is not None:
            return typ
        else:
            logger.info("unexpected column: %s", c)
            return c.type.python_type.__name__  # xxx


def normalize_name(nameset_list):
    r = []
    arrived = set()
    for nameset in nameset_list:
        nameset = singularize(nameset.replace("_id", "").replace("pokemon_v2_", ""))
        for name in nameset.split("_"):
            if name not in arrived:
                arrived.add(name)
                r.append(name)
    return "_".join(r)


def name_for_scalar_relationship(base, local_cls, referred_cls, constraint):
    itr = itertools.chain(
        [referred_cls.__name__],
        [
            col.table.name for col in constraint
            if col.table not in (local_cls.__table__, referred_cls.__table__)
        ],
        [col.name for col in constraint if col.table == local_cls.__table__],
    )
    return normalize_name(itr)


def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    itr = itertools.chain(
        [
            col.table.name for col in constraint
            if col.table not in (local_cls.__table__, referred_cls.__table__)
        ],
        [col.name for col in constraint if col.table == referred_cls.__table__],
        [referred_cls.__name__],
    )
    return pluralize(normalize_name(itr))


def classname_for_table(tablename):
    return camelize(normalize_name([tablename]))


def main(src):
    Base = automap.automap_base()
    engine = create_engine(src)
    Base.prepare(
        engine,
        reflect=True,
        name_for_scalar_relationship=name_for_scalar_relationship,
        name_for_collection_relationship=name_for_collection_relationship,
    )
    collector = Collector(Resolver())
    d = collector.collect(sorted(Base.classes, key=lambda x: x.__table__.fullname))
    loading.dumpfile(d, format="json")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="sqlite:///./dog.db")
    args = parser.parse_args()
    main(args.src)

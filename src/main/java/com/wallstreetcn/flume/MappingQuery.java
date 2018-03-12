package com.wallstreetcn.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Event;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Query;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

public class MappingQuery {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSink.class);
    private static final String HEADER_PREFIX = "header.";
    private static final String BODY_PREFIX = "body.";

    /**
     * Render the mapping with flume event represented as header part and body part,
     * the mapping is separated by colon, and first part is field name, the second is header
     * or body part and the third is type of the field, for example,
     * field1:header.field1:string,field2:body.field1:int
     *
     * @param dslContext dsl context
     * @param mapping    the data insertion
     * @param events     the events
     * @param bodyFormat body format is `json` or `string`, when json the event's body will be
     *                   decoded using `JSON` format.
     * @return jooq query
     */
    public static List<Query> render(DSLContext dslContext, String table, String mapping, List<Event> events, String bodyFormat) {
        switch (bodyFormat.toLowerCase()) {
            case "json":
                return renderJSON(dslContext, table, mapping, events);
        }
        return null;
    }

    private static List<Query> renderJSON(DSLContext dslContext, String table, String mappings, List<Event> events) {
        List<Query> queries = new ArrayList<>();
        events.forEach(e -> {
            JSONObject o = JSON.parseObject(new String(e.getBody()));

            List<Field<Object>> fields = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String mapping : mappings.split(",")) {
                String[] parts = mapping.split(":");
                if (parts.length != 2) {
                    LOG.error("fail to resolve the part");
                }

                String field = parts[0];
                String key = parts[1];
                Object val = null;
                if (key.startsWith(HEADER_PREFIX)) {
                    val = fetchFromHeader(e, key);
                } else if (key.startsWith(BODY_PREFIX)) {
                    val = fetchFromBody(o, key);
                }
                fields.add(DSL.field(name(field)));
                values.add(val);
            }

            Query q = dslContext.insertInto(table(table)).columns(fields).values(values);
            queries.add(q);
            LOG.debug("inlined SQL", q.getSQL(ParamType.INLINED));
        });
        return queries;
    }

    private static Object fetchFromHeader(Event e, String key) {
        return e.getHeaders().get(key.substring(HEADER_PREFIX.length()));
    }

    private static Object fetchFromBody(JSONObject o, String key) {
        return o.get(key.substring(BODY_PREFIX.length()));
    }
}

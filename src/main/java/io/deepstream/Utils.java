package io.deepstream;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

class Utils {
    static <E extends Enum<E>> Map<String, E> createEnumLookup(Class<E> enumClass) {
        return EnumSet.allOf(enumClass).stream()
                .collect(Collectors.toMap(Enum::toString, Function.identity()));
    }

    static JsonElement ensureValidJsonObject(JsonElement authParameters) {
        return Optional.ofNullable(authParameters).orElseGet(JsonObject::new);
    }
}

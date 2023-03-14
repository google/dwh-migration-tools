package com.google.edwmigration.dumper.application.dumper.connector;

import java.util.function.BiPredicate;

public interface SqlQueryFactory {
  String getSql(BiPredicate<String, String> validator);
}

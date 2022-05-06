package com.wenbao.flink.mysql.source.core;

import java.io.Serializable;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ColumnHandleInternal implements Serializable {

  private final String name;
  private final String type;
  private final int ordinalPosition;

  public ColumnHandleInternal(String name, String type, int ordinalPosition) {
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
    this.ordinalPosition = ordinalPosition;
  }

  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, ordinalPosition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }

    ColumnHandleInternal other = (ColumnHandleInternal) obj;
    return Objects.equals(this.name, other.name)
        && Objects.equals(this.type, other.type)
        && Objects.equals(this.ordinalPosition, other.ordinalPosition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("name", name)
        .add("type", type)
        .add("ordinalPosition", ordinalPosition)
        .toString();
  }
}
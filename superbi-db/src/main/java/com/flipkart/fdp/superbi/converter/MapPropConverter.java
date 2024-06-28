package com.flipkart.fdp.superbi.converter;

import com.flipkart.fdp.superbi.utils.MapUtil;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import org.apache.commons.lang3.StringUtils;

@Converter(autoApply = true)
public class MapPropConverter implements AttributeConverter<Map<String,String> , String> {

  @Override
  public String convertToDatabaseColumn(Map<String, String> federationPropMap) {
    return MapUtil.MapToString(federationPropMap);
  }

  @Override
  public Map<String, String> convertToEntityAttribute(String federationProp) {
    if (StringUtils.isNotBlank(federationProp) ) {
      return MapUtil.stringToMap(federationProp);
    }
    return Maps.newHashMap();
  }
}

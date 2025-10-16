package mysql2clickhouse

import (
	"fmt"
	"github.com/zeromicro/go-zero/core/logx"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coorrer/erebus/internal/config"
)

// MappedData 映射后的数据
type MappedData struct {
	Columns []string
	Rows    []map[string]interface{}
}

// FieldMapper 字段映射器
type FieldMapper struct {
	config          config.SyncTaskTable
	transformers    map[string]func(interface{}) (interface{}, error)
	enumMappings    map[string]map[string]interface{} // field_name -> (source_value -> target_value)
	reverseEnumMaps map[string]map[interface{}]string // field_name -> (target_value -> source_value) 用于反向查找
}

// NewFieldMapper 创建字段映射器
func NewFieldMapper(tableConfig config.SyncTaskTable) *FieldMapper {
	mapper := &FieldMapper{
		config:          tableConfig,
		transformers:    make(map[string]func(interface{}) (interface{}, error)),
		enumMappings:    make(map[string]map[string]interface{}),
		reverseEnumMaps: make(map[string]map[interface{}]string),
	}

	// 初始化内置转换器
	mapper.initTransformers()
	// 初始化枚举映射
	mapper.initEnumMappings()

	return mapper
}

// initEnumMappings 初始化枚举映射 - 只从ColumnMapping中读取
func (m *FieldMapper) initEnumMappings() {
	// 只从列映射中初始化枚举映射
	for _, mapping := range m.config.ColumnMappings {
		// 判断 EnumMapping 是否不为空来确定是否是枚举字段
		if mapping.EnumMapping != nil && len(mapping.EnumMapping) > 0 {
			enumMap := make(map[string]interface{})
			reverseMap := make(map[interface{}]string)

			for _, enumDef := range mapping.EnumMapping {
				enumMap[enumDef.Source] = enumDef.Target
				reverseMap[enumDef.Target] = enumDef.Source
			}

			// 使用源字段名作为key
			key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + mapping.Source
			m.enumMappings[key] = enumMap
			m.reverseEnumMaps[key] = reverseMap

			logx.Infof("Initialized enum mapping for field %s with %d mappings",
				mapping.Source, len(mapping.EnumMapping))
		}
	}
}

// MapRow 映射单行数据 - 支持通用枚举映射
func (m *FieldMapper) MapRow(sourceRow map[string]interface{}) (map[string]interface{}, error) {
	mappedRow := make(map[string]interface{})

	for _, mapping := range m.config.ColumnMappings {
		if mapping.Ignore {
			continue
		}

		// 获取源值
		sourceValue, exists := sourceRow[mapping.Source]

		// 检查必需字段
		if mapping.Required && (!exists || sourceValue == nil || sourceValue == "") {
			return nil, fmt.Errorf("required field %s is missing or empty", mapping.Source)
		}

		// 应用条件检查
		if mapping.Condition != "" {
			conditionMet, err := m.evaluateCondition(mapping.Condition, sourceValue)
			if err != nil {
				return nil, fmt.Errorf("error evaluating condition for %s: %v", mapping.Source, err)
			}
			if !conditionMet {
				continue
			}
		}

		// 应用转换或使用默认值
		var finalValue interface{}
		var err error

		if !exists || sourceValue == nil || sourceValue == "" {
			// 使用默认值
			finalValue = mapping.DefaultValue
		} else if mapping.Transform != "" {
			// 应用转换
			finalValue, err = m.applyTransform(mapping.Transform, sourceValue)
			if err != nil {
				return nil, fmt.Errorf("error transforming field %s: %v", mapping.Source, err)
			}
		} else if m.isEnumField(mapping.Source) {
			// 处理枚举映射
			finalValue, err = m.transformEnum(mapping.Source, sourceValue)
			if err != nil {
				return nil, fmt.Errorf("error transforming enum field %s: %v", mapping.Source, err)
			}
		} else {
			// 直接使用源值
			finalValue = sourceValue
		}

		// 类型转换
		if mapping.Type != "" {
			finalValue, err = m.convertType(finalValue, mapping.Type)
			if err != nil {
				return nil, fmt.Errorf("error converting type for field %s: %v", mapping.Source, err)
			}
		}

		mappedRow[mapping.Target] = finalValue
	}

	return mappedRow, nil
}

// isEnumField 检查字段是否是枚举字段
func (m *FieldMapper) isEnumField(fieldName string) bool {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	_, exists := m.enumMappings[key]
	return exists
}

// transformEnum 通用枚举转换方法
func (m *FieldMapper) transformEnum(fieldName string, value interface{}) (interface{}, error) {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	enumMap, exists := m.enumMappings[key]
	if !exists {
		return value, fmt.Errorf("no enum mapping found for field %s", fieldName)
	}

	// 将输入值转换为字符串进行匹配
	strValue, err := m.valueToString(value)
	if err != nil {
		return value, fmt.Errorf("failed to convert value to string for enum field %s: %v", fieldName, err)
	}

	// 查找枚举映射
	if enumValue, exists := enumMap[strValue]; exists {
		logx.Debugf("Transformed enum field %s: %s -> %v", fieldName, strValue, enumValue)
		return enumValue, nil
	}

	// 如果没有找到映射，尝试使用默认值
	if defaultVal, exists := enumMap["__default__"]; exists {
		logx.Errorf("Using default value for enum field %s: %s -> %v", fieldName, strValue, defaultVal)
		return defaultVal, nil
	}

	// 如果都没有找到，返回原始值并记录警告
	logx.Errorf("No enum mapping found for value '%s' in field %s, using original value", strValue, fieldName)
	return value, nil
}

// valueToString 将任意值转换为字符串
func (m *FieldMapper) valueToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%f", v), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// ReverseMapEnum 反向枚举映射（用于调试和日志）
func (m *FieldMapper) ReverseMapEnum(fieldName string, targetValue interface{}) (string, error) {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	reverseMap, exists := m.reverseEnumMaps[key]
	if !exists {
		return "", fmt.Errorf("no reverse enum mapping found for field %s", fieldName)
	}

	if sourceValue, exists := reverseMap[targetValue]; exists {
		return sourceValue, nil
	}

	return "", fmt.Errorf("no reverse mapping found for target value %v in field %s", targetValue, fieldName)
}

// GetEnumMappingInfo 获取枚举映射信息（用于监控和调试）
func (m *FieldMapper) GetEnumMappingInfo() map[string]interface{} {
	info := make(map[string]interface{})

	for key, enumMap := range m.enumMappings {
		info[key] = enumMap
	}

	return info
}

// GetEnumFields 获取所有枚举字段列表
func (m *FieldMapper) GetEnumFields() []string {
	var enumFields []string
	for key := range m.enumMappings {
		// 从key中提取字段名: database.table.field
		parts := strings.Split(key, ".")
		if len(parts) == 3 {
			enumFields = append(enumFields, parts[2])
		}
	}
	return enumFields
}

// 以下为原有的转换器方法，保持不变
func (m *FieldMapper) initTransformers() {
	m.transformers = map[string]func(interface{}) (interface{}, error){
		"parseDateTimeBestEffort": m.transformDateTime,
		"toUnixTimestamp":         m.transformToUnixTimestamp,
		"toLowerCase":             m.transformToLowerCase,
		"toUpperCase":             m.transformToUpperCase,
		"trim":                    m.transformTrim,
		"if":                      m.transformIf,
		"default":                 m.transformDefault,
		"toEnum":                  m.transformToEnum,      // 枚举转换器
		"enum":                    m.transformEnumGeneric, // 通用枚举转换器
	}
}

// transformEnumGeneric 通用枚举转换器，可用于transform字段
func (m *FieldMapper) transformEnumGeneric(value interface{}) (interface{}, error) {
	// 这个转换器需要配合参数使用，例如：enum('field_name')
	// 在实际使用中，需要通过更复杂的方式解析参数
	return value, nil
}

// transformToEnum 枚举转换器（带字段名参数）
func (m *FieldMapper) transformToEnum(value interface{}) (interface{}, error) {
	// 这个转换器需要在调用时指定字段名
	return value, nil
}

// 原有的转换方法保持不变
func (m *FieldMapper) applyTransform(transform string, value interface{}) (interface{}, error) {
	expr := strings.ReplaceAll(transform, "{{value}}", fmt.Sprintf("%v", value))

	if strings.Contains(expr, "(") {
		return m.executeFunction(expr)
	}

	return m.evaluateExpression(expr)
}

func (m *FieldMapper) executeFunction(expr string) (interface{}, error) {
	re := regexp.MustCompile(`(\w+)\(([^)]*)\)`)
	matches := re.FindStringSubmatch(expr)
	if len(matches) < 3 {
		return expr, nil
	}

	funcName := matches[1]
	paramsStr := matches[2]

	params := strings.Split(paramsStr, ",")
	for i, param := range params {
		params[i] = strings.TrimSpace(param)
	}

	if transformer, exists := m.transformers[funcName]; exists {
		if len(params) > 0 {
			return transformer(params[0])
		}
		return transformer(nil)
	}

	return expr, nil
}

func (m *FieldMapper) evaluateExpression(expr string) (interface{}, error) {
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}

	if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return num, nil
	}
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num, nil
	}

	if strings.HasPrefix(expr, `"`) && strings.HasSuffix(expr, `"`) {
		return expr[1 : len(expr)-1], nil
	}
	if strings.HasPrefix(expr, `'`) && strings.HasSuffix(expr, `'`) {
		return expr[1 : len(expr)-1], nil
	}

	return expr, nil
}

func (m *FieldMapper) evaluateCondition(condition string, value interface{}) (bool, error) {
	expr := strings.ReplaceAll(condition, "{{value}}", fmt.Sprintf("%v", value))

	if strings.Contains(expr, "!=") {
		parts := strings.Split(expr, "!=")
		if len(parts) == 2 {
			return strings.TrimSpace(parts[0]) != strings.TrimSpace(parts[1]), nil
		}
	}

	if strings.Contains(expr, "==") {
		parts := strings.Split(expr, "==")
		if len(parts) == 2 {
			return strings.TrimSpace(parts[0]) == strings.TrimSpace(parts[1]), nil
		}
	}

	if strings.Contains(expr, ">") {
		parts := strings.Split(expr, ">")
		if len(parts) == 2 {
			left, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
			if err != nil {
				return false, err
			}
			right, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				return false, err
			}
			return left > right, nil
		}
	}

	return value != nil && value != "", nil
}

func (m *FieldMapper) convertType(value interface{}, targetType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch strings.ToLower(targetType) {
	case "uint8", "uint16", "uint32", "uint64":
		return m.convertToUint(value, targetType)
	case "int8", "int16", "int32", "int64":
		return m.convertToInt(value, targetType)
	case "float32", "float64":
		return m.convertToFloat(value, targetType)
	case "string":
		return fmt.Sprintf("%v", value), nil
	case "datetime":
		return m.convertToDateTime(value)
	case "date":
		return m.convertToDate(value)
	case "bool", "boolean":
		return m.convertToBool(value)
	default:
		return value, nil
	}
}

func (m *FieldMapper) convertToUint(value interface{}, targetType string) (uint64, error) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return uint64(v.(int64)), nil
	case uint, uint8, uint16, uint32, uint64:
		return v.(uint64), nil
	case float32, float64:
		return uint64(v.(float64)), nil
	case string:
		if v == "" {
			return 0, nil
		}
		result, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return result, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to %s", value, targetType)
	}
}

func (m *FieldMapper) convertToInt(value interface{}, targetType string) (int64, error) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return v.(int64), nil
	case uint, uint8, uint16, uint32, uint64:
		return int64(v.(uint64)), nil
	case float32, float64:
		return int64(v.(float64)), nil
	case string:
		if v == "" {
			return 0, nil
		}
		result, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, err
		}
		return result, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to %s", value, targetType)
	}
}

func (m *FieldMapper) convertToFloat(value interface{}, targetType string) (float64, error) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return float64(v.(int64)), nil
	case uint, uint8, uint16, uint32, uint64:
		return float64(v.(uint64)), nil
	case float32, float64:
		return v.(float64), nil
	case string:
		if v == "" {
			return 0, nil
		}
		result, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, err
		}
		return result, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to %s", value, targetType)
	}
}

func (m *FieldMapper) convertToDateTime(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		if v == "" {
			return time.Time{}, nil
		}
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02",
			time.RFC3339,
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse datetime: %s", v)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to datetime", value)
	}
}

func (m *FieldMapper) convertToDate(value interface{}) (time.Time, error) {
	t, err := m.convertToDateTime(value)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
}

func (m *FieldMapper) convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64:
		return v.(int64) != 0, nil
	case uint, uint8, uint16, uint32, uint64:
		return v.(uint64) != 0, nil
	case float32, float64:
		return v.(float64) != 0, nil
	case string:
		lower := strings.ToLower(v)
		return lower == "true" || lower == "1" || lower == "yes" || lower == "on", nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// 内置转换器实现
func (m *FieldMapper) transformDateTime(value interface{}) (interface{}, error) {
	return m.convertToDateTime(value)
}

func (m *FieldMapper) transformToUnixTimestamp(value interface{}) (interface{}, error) {
	t, err := m.convertToDateTime(value)
	if err != nil {
		return nil, err
	}
	return t.Unix(), nil
}

func (m *FieldMapper) transformToLowerCase(value interface{}) (interface{}, error) {
	return strings.ToLower(fmt.Sprintf("%v", value)), nil
}

func (m *FieldMapper) transformToUpperCase(value interface{}) (interface{}, error) {
	return strings.ToUpper(fmt.Sprintf("%v", value)), nil
}

func (m *FieldMapper) transformTrim(value interface{}) (interface{}, error) {
	return strings.TrimSpace(fmt.Sprintf("%v", value)), nil
}

func (m *FieldMapper) transformIf(value interface{}) (interface{}, error) {
	return value, nil
}

func (m *FieldMapper) transformDefault(value interface{}) (interface{}, error) {
	if value == nil || value == "" {
		return value, nil
	}
	return value, nil
}

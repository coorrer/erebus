package mysql2clickhouse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coorrer/erebus/internal/config"
	"github.com/zeromicro/go-zero/core/logx"
)

// FieldMapper 字段映射器
type FieldMapper struct {
	config       config.SyncTaskTable
	transformers map[string]func(interface{}) (interface{}, error)

	// 枚举映射：字段名 -> (MySQL索引 -> ClickHouse常量)
	enumMappings    map[string]map[int64]string
	reverseEnumMaps map[string]map[string]int64
	defaultValues   map[string]string

	// SET 映射：字段名 -> (位值 -> SET常量)
	setMappings   map[string]map[int64]string
	setConstToBit map[string]map[string]int64
	setDefaults   map[string]string
}

// NewFieldMapper 创建字段映射器
func NewFieldMapper(tableConfig config.SyncTaskTable) *FieldMapper {
	mapper := &FieldMapper{
		config:          tableConfig,
		transformers:    make(map[string]func(interface{}) (interface{}, error)),
		enumMappings:    make(map[string]map[int64]string),
		reverseEnumMaps: make(map[string]map[string]int64),
		defaultValues:   make(map[string]string),
		setMappings:     make(map[string]map[int64]string),
		setConstToBit:   make(map[string]map[string]int64),
		setDefaults:     make(map[string]string),
	}

	// 初始化内置转换器
	mapper.initTransformers()
	// 初始化枚举映射
	mapper.initEnumMappings()
	// 初始化 SET 映射
	mapper.initSetMappings()

	return mapper
}

// initTransformers 初始化内置转换器
func (m *FieldMapper) initTransformers() {
	m.transformers = map[string]func(interface{}) (interface{}, error){
		"parseDateTimeBestEffort": m.transformDateTime,
		"toUnixTimestamp":         m.transformToUnixTimestamp,
		"toLowerCase":             m.transformToLowerCase,
		"toUpperCase":             m.transformToUpperCase,
		"trim":                    m.transformTrim,
		"if":                      m.transformIf,
		"default":                 m.transformDefault,
		"toEnum":                  m.transformToEnum,
		"enum":                    m.transformEnumGeneric,
	}
}

// initEnumMappings 初始化枚举映射
func (m *FieldMapper) initEnumMappings() {
	for _, mapping := range m.config.ColumnMappings {
		// 判断是否是枚举字段
		if mapping.EnumMapping != nil && len(mapping.EnumMapping) > 0 {
			// 初始化枚举映射
			enumMap := make(map[int64]string)
			reverseMap := make(map[string]int64)

			for _, enumDef := range mapping.EnumMapping {
				// 检查是否是默认值定义
				if enumDef.Index == -1 {
					m.defaultValues[mapping.Source] = enumDef.Const
					logx.Infof("Set default value for enum field %s: %s", mapping.Source, enumDef.Const)
					continue
				}

				// 正常枚举映射
				enumMap[enumDef.Index] = enumDef.Const
				reverseMap[enumDef.Const] = enumDef.Index
			}

			key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + mapping.Source
			m.enumMappings[key] = enumMap
			m.reverseEnumMaps[key] = reverseMap

			logx.Infof("Initialized enum mapping for field %s with %d mappings",
				mapping.Source, len(enumMap))
		}
	}
}

// initSetMappings 初始化 SET 类型映射
func (m *FieldMapper) initSetMappings() {
	for _, mapping := range m.config.ColumnMappings {
		// 判断是否是 SET 字段
		if mapping.SetMapping != nil && len(mapping.SetMapping) > 0 {
			// 初始化 SET 映射
			setMap := make(map[int64]string)
			constToBitMap := make(map[string]int64)

			for _, setDef := range mapping.SetMapping {
				// 检查是否是默认值定义
				if setDef.Bit == -1 {
					m.setDefaults[mapping.Source] = setDef.Const
					logx.Infof("Set default value for SET field %s: %s", mapping.Source, setDef.Const)
					continue
				}

				// 正常 SET 映射
				setMap[setDef.Bit] = setDef.Const
				constToBitMap[setDef.Const] = setDef.Bit
			}

			key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + mapping.Source
			m.setMappings[key] = setMap
			m.setConstToBit[key] = constToBitMap

			logx.Infof("Initialized SET mapping for field %s with %d mappings",
				mapping.Source, len(setMap))
		}
	}
}

// MapRow 映射单行数据 - 根据 Type 字段控制枚举和 SET 转换
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
		} else if m.shouldTransformEnum(mapping) {
			// 只有当 Type 为 "enum2string" 时才进行枚举转换
			finalValue, err = m.transformEnum(mapping.Source, sourceValue)
			if err != nil {
				return nil, fmt.Errorf("error transforming enum field %s: %v", mapping.Source, err)
			}
		} else if m.shouldTransformSet(mapping) {
			// 只有当 Type 为 "setString" 时才进行 SET 转换
			finalValue, err = m.transformSet(mapping.Source, sourceValue)
			if err != nil {
				return nil, fmt.Errorf("error transforming set field %s: %v", mapping.Source, err)
			}
		} else {
			// 直接使用源值
			finalValue = sourceValue
		}

		// 类型转换（跳过 enum2string 和 setString 类型，因为已经在上面处理了）
		if mapping.Type != "" && mapping.Type != "enum2string" && mapping.Type != "setString" {
			finalValue, err = m.convertType(finalValue, mapping.Type)
			if err != nil {
				return nil, fmt.Errorf("error converting type for field %s: %v", mapping.Source, err)
			}
		}

		mappedRow[mapping.Target] = finalValue
	}

	return mappedRow, nil
}

// shouldTransformEnum 判断是否应该进行枚举转换
func (m *FieldMapper) shouldTransformEnum(mapping config.ColumnMapping) bool {
	// 只有当 Type 为 "enum2string" 并且有枚举映射时才进行转换
	return strings.ToLower(mapping.Type) == "enum2string" &&
		m.isEnumField(mapping.Source)
}

// shouldTransformSet 判断是否应该进行 SET 转换
func (m *FieldMapper) shouldTransformSet(mapping config.ColumnMapping) bool {
	// 只有当 Type 为 "setString" 并且有 SET 映射时才进行转换
	return strings.ToLower(mapping.Type) == "setstring" &&
		m.isSetField(mapping.Source)
}

// isEnumField 检查字段是否是枚举字段
func (m *FieldMapper) isEnumField(fieldName string) bool {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	_, exists := m.enumMappings[key]
	return exists
}

// isSetField 检查字段是否是 SET 字段
func (m *FieldMapper) isSetField(fieldName string) bool {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	_, exists := m.setMappings[key]
	return exists
}

// transformEnum 枚举转换方法 - 直接从索引映射到常量
func (m *FieldMapper) transformEnum(fieldName string, value interface{}) (string, error) {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	enumMap, exists := m.enumMappings[key]
	if !exists {
		return "", fmt.Errorf("no enum mapping found for field %s", fieldName)
	}

	// 将输入值转换为整数索引
	index, err := m.valueToInt64(value)
	if err != nil {
		return "", fmt.Errorf("failed to convert value to int64 for enum field %s: %v", fieldName, err)
	}

	// 查找枚举映射
	if constValue, exists := enumMap[index]; exists {
		logx.Debugf("Transformed enum field %s: index %d -> const '%s'",
			fieldName, index, constValue)
		return constValue, nil
	}

	// 如果没有找到映射，尝试使用默认值
	if defaultVal, exists := m.defaultValues[fieldName]; exists {
		logx.Errorf("Using default value for enum field %s: MySQL index %d -> CH const '%s'",
			fieldName, index, defaultVal)
		return defaultVal, nil
	}

	// 如果都没有找到，返回错误
	return "", fmt.Errorf("no enum mapping found for MySQL index %d in field %s", index, fieldName)
}

// transformSet SET 类型转换方法 - 将位图值转换为字符串
func (m *FieldMapper) transformSet(fieldName string, value interface{}) (string, error) {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	setMap, exists := m.setMappings[key]
	if !exists {
		return "", fmt.Errorf("no SET mapping found for field %s", fieldName)
	}

	// 将输入值转换为整数位图
	bitmap, err := m.valueToInt64(value)
	if err != nil {
		return "", fmt.Errorf("failed to convert value to int64 for SET field %s: %v", fieldName, err)
	}

	// 如果位图为0，返回空字符串
	if bitmap == 0 {
		return "", nil
	}

	// 收集所有设置的位对应的常量
	var setValues []string
	for bit, constValue := range setMap {
		// 检查该位是否被设置
		if bitmap&bit != 0 {
			setValues = append(setValues, constValue)
		}
	}

	// 如果没有匹配的值，使用默认值
	if len(setValues) == 0 {
		if defaultVal, exists := m.setDefaults[fieldName]; exists {
			logx.Errorf("Using default value for SET field %s: bitmap %d -> '%s'",
				fieldName, bitmap, defaultVal)
			return defaultVal, nil
		}
		return "", nil
	}

	// 返回逗号分隔的字符串
	result := strings.Join(setValues, ",")
	logx.Debugf("Transformed SET field %s: bitmap %d -> '%s'", fieldName, bitmap, result)
	return result, nil
}

// valueToInt64 将任意值转换为int64
func (m *FieldMapper) valueToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
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
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// applyTransform 应用转换表达式
func (m *FieldMapper) applyTransform(transform string, value interface{}) (interface{}, error) {
	expr := strings.ReplaceAll(transform, "{{value}}", fmt.Sprintf("%v", value))

	if strings.Contains(expr, "(") {
		return m.executeFunction(expr)
	}

	return m.evaluateExpression(expr)
}

// executeFunction 执行函数调用
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

// evaluateExpression 评估表达式
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

// evaluateCondition 评估条件表达式
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

// convertType 类型转换
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

// convertToUint 转换为无符号整数
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

// convertToInt 转换为整数
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

// convertToFloat 转换为浮点数
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

// convertToDateTime 转换为日期时间
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

// convertToDate 转换为日期
func (m *FieldMapper) convertToDate(value interface{}) (time.Time, error) {
	t, err := m.convertToDateTime(value)
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()), nil
}

// convertToBool 转换为布尔值
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

func (m *FieldMapper) transformToEnum(value interface{}) (interface{}, error) {
	return value, nil
}

func (m *FieldMapper) transformEnumGeneric(value interface{}) (interface{}, error) {
	return value, nil
}

// ReverseMapEnum 反向枚举映射（用于调试和日志）
func (m *FieldMapper) ReverseMapEnum(fieldName string, constValue string) (int64, error) {
	key := m.config.SourceDatabase + "." + m.config.SourceTable + "." + fieldName
	reverseMap, exists := m.reverseEnumMaps[key]
	if !exists {
		return 0, fmt.Errorf("no reverse enum mapping found for field %s", fieldName)
	}

	if index, exists := reverseMap[constValue]; exists {
		return index, nil
	}

	return 0, fmt.Errorf("no reverse mapping found for const value '%s' in field %s", constValue, fieldName)
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

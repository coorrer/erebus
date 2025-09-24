package mysql2meilisearch

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coorrer/erebus/internal/config"
)

// FieldMapper 字段映射器
type FieldMapper struct {
	config       config.SyncTaskTable
	transformers map[string]func(interface{}) (interface{}, error)
}

// NewFieldMapper 创建字段映射器
func NewFieldMapper(tableConfig config.SyncTaskTable) *FieldMapper {
	mapper := &FieldMapper{
		config:       tableConfig,
		transformers: make(map[string]func(interface{}) (interface{}, error)),
	}

	// 初始化内置转换器
	mapper.initTransformers()

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
		"concat":                  m.transformConcat,
		"substring":               m.transformSubstring,
		"replace":                 m.transformReplace,
		"jsonStringify":           m.transformJSONStringify,
		"toBoolean":               m.transformToBoolean,
	}
}

// MapRow 映射单行数据
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

// applyTransform 应用转换表达式
func (m *FieldMapper) applyTransform(transform string, value interface{}) (interface{}, error) {
	// 替换变量占位符
	expr := strings.ReplaceAll(transform, "{{value}}", fmt.Sprintf("%v", value))

	// 检查是否是函数调用
	if strings.Contains(expr, "(") {
		return m.executeFunction(expr)
	}

	// 简单表达式处理
	return m.evaluateExpression(expr)
}

// executeFunction 执行函数调用
func (m *FieldMapper) executeFunction(expr string) (interface{}, error) {
	// 解析函数名和参数
	re := regexp.MustCompile(`(\w+)\(([^)]*)\)`)
	matches := re.FindStringSubmatch(expr)
	if len(matches) < 3 {
		return expr, nil
	}

	funcName := matches[1]
	paramsStr := matches[2]

	// 分割参数
	params := strings.Split(paramsStr, ",")
	for i, param := range params {
		params[i] = strings.TrimSpace(param)
	}

	// 执行内置函数
	if transformer, exists := m.transformers[funcName]; exists {
		// 对于单参数函数，使用第一个参数
		if len(params) > 0 {
			return transformer(params[0])
		}
		return transformer(nil)
	}

	return expr, nil
}

// evaluateExpression 评估表达式
func (m *FieldMapper) evaluateExpression(expr string) (interface{}, error) {
	// 处理布尔表达式
	if expr == "true" {
		return true, nil
	}
	if expr == "false" {
		return false, nil
	}

	// 处理数字
	if num, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return num, nil
	}
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num, nil
	}

	// 处理字符串（移除可能的引号）
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
	// 替换变量占位符
	expr := strings.ReplaceAll(condition, "{{value}}", fmt.Sprintf("%v", value))

	// 简单条件处理
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

	if strings.Contains(expr, "<") {
		parts := strings.Split(expr, "<")
		if len(parts) == 2 {
			left, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
			if err != nil {
				return false, err
			}
			right, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				return false, err
			}
			return left < right, nil
		}
	}

	if strings.Contains(expr, ">=") {
		parts := strings.Split(expr, ">=")
		if len(parts) == 2 {
			left, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
			if err != nil {
				return false, err
			}
			right, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				return false, err
			}
			return left >= right, nil
		}
	}

	if strings.Contains(expr, "<=") {
		parts := strings.Split(expr, "<=")
		if len(parts) == 2 {
			left, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
			if err != nil {
				return false, err
			}
			right, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				return false, err
			}
			return left <= right, nil
		}
	}

	// 默认处理为空检查
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
		// 尝试多种日期格式
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02",
			time.RFC3339,
			"2006-01-02 15:04:05.999999",
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
	// 只保留日期部分
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
		return lower == "true" || lower == "1" || lower == "yes" || lower == "on" || lower == "y", nil
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
	// 解析 if(condition, true_value, false_value) 格式
	// 这里简化处理，实际应该解析参数
	return value, nil
}

func (m *FieldMapper) transformDefault(value interface{}) (interface{}, error) {
	// 如果值为空，使用默认值
	if value == nil || value == "" {
		return value, nil // 实际应该返回配置的默认值
	}
	return value, nil
}

func (m *FieldMapper) transformConcat(value interface{}) (interface{}, error) {
	// 解析 concat(value1, value2, ...) 格式
	// 这里简化处理，实际应该解析参数
	return value, nil
}

func (m *FieldMapper) transformSubstring(value interface{}) (interface{}, error) {
	// 解析 substring(value, start, length) 格式
	// 这里简化处理，实际应该解析参数
	return value, nil
}

func (m *FieldMapper) transformReplace(value interface{}) (interface{}, error) {
	// 解析 replace(value, old, new) 格式
	// 这里简化处理，实际应该解析参数
	return value, nil
}

func (m *FieldMapper) transformJSONStringify(value interface{}) (interface{}, error) {
	// 将值转换为 JSON 字符串
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value to JSON: %v", err)
	}
	return string(jsonBytes), nil
}

func (m *FieldMapper) transformToBoolean(value interface{}) (interface{}, error) {
	return m.convertToBool(value)
}

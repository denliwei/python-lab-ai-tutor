"""
代码运行沙箱模块 - 安全地执行学生Python代码
支持：超时控制、内存限制、输出捕获、安全检查
"""
import subprocess
import sys
import tempfile
import os
import re
import signal
import traceback
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from config import Config


@dataclass
class RunResult:
    """代码运行结果"""
    success: bool              # 是否成功执行
    output: str                # 标准输出
    error: str                 # 错误信息
    return_code: int           # 返回码
    execution_time: float      # 执行时间（秒）
    timed_out: bool           # 是否超时
    memory_exceeded: bool     # 是否内存超限
    security_blocked: bool    # 是否被安全检查拦截
    blocked_reason: str       # 拦截原因


class CodeSecurityChecker:
    """代码安全检查器"""
    
    # 危险的导入模块
    DANGEROUS_IMPORTS = [
        'os', 'sys', 'subprocess', 'shutil', 'socket', 'requests',
        'urllib', 'http', 'ftplib', 'smtplib', 'telnetlib',
        'pickle', 'shelve', 'marshal', 'importlib',
        'ctypes', 'multiprocessing', 'threading',
        'asyncio', 'concurrent', 'signal',
        'resource', 'pty', 'fcntl', 'termios',
        'code', 'codeop', 'compileall', 'py_compile',
        'builtins', '__builtins__',
    ]
    
    # 危险的函数调用
    DANGEROUS_CALLS = [
        'eval', 'exec', 'compile', '__import__',
        'open', 'file', 'input',  # input可以根据需要放开
        'getattr', 'setattr', 'delattr',
        'globals', 'locals', 'vars',
        'breakpoint', 'exit', 'quit',
    ]
    
    # 危险的模式（正则表达式）
    DANGEROUS_PATTERNS = [
        r'__\w+__',           # 双下划线魔术方法/属性
        r'lambda.*exec',      # lambda中执行代码
        r'lambda.*eval',
        r'\.read\s*\(',       # 文件读取
        r'\.write\s*\(',      # 文件写入
        r'subprocess\.',
        r'os\.',
        r'sys\.',
    ]
    
    @classmethod
    def check_code(cls, code: str) -> Tuple[bool, str]:
        """
        检查代码安全性
        
        Args:
            code: 要检查的代码
            
        Returns:
            (is_safe, reason) - 是否安全及原因
        """
        # 检查导入
        import_pattern = r'(?:^|\s)(?:import|from)\s+(\w+)'
        imports = re.findall(import_pattern, code, re.MULTILINE)
        
        for imp in imports:
            if imp in cls.DANGEROUS_IMPORTS:
                return False, f"禁止导入模块: {imp}"
        
        # 检查危险函数调用
        for func in cls.DANGEROUS_CALLS:
            # 只有在配置允许时才放行input
            if func == 'input':
                continue  # 允许input用于交互式题目
            pattern = rf'\b{func}\s*\('
            if re.search(pattern, code):
                return False, f"禁止使用函数: {func}()"
        
        # 检查危险模式
        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, code):
                # 允许__name__ == '__main__'这种常见用法
                if pattern == r'__\w+__' and re.search(r'__name__\s*==\s*[\'"]__main__[\'"]', code):
                    continue
                if pattern == r'__\w+__' and re.search(r'def\s+__\w+__\s*\(', code):
                    continue  # 允许定义魔术方法
                return False, f"检测到不允许的代码模式"
        
        return True, ""


class CodeRunner:
    """
    代码运行器 - 在受限环境中执行Python代码
    """
    
    def __init__(self):
        self.timeout = getattr(Config, 'CODE_RUNNER_TIMEOUT', 5)
        self.max_output = getattr(Config, 'CODE_RUNNER_MAX_OUTPUT', 10000)
        self.enabled = getattr(Config, 'CODE_RUNNER_ENABLED', True)
    
    def run(self, code: str, stdin_input: str = "", test_cases: List[Dict] = None) -> RunResult:
        """
        运行代码
        
        Args:
            code: 要执行的Python代码
            stdin_input: 标准输入内容
            test_cases: 测试用例列表 [{"input": "...", "expected": "..."}, ...]
            
        Returns:
            RunResult 对象
        """
        if not self.enabled:
            return RunResult(
                success=False,
                output="",
                error="代码运行功能已禁用",
                return_code=-1,
                execution_time=0,
                timed_out=False,
                memory_exceeded=False,
                security_blocked=True,
                blocked_reason="代码运行功能已禁用"
            )
        
        # 安全检查
        is_safe, reason = CodeSecurityChecker.check_code(code)
        if not is_safe:
            return RunResult(
                success=False,
                output="",
                error=f"安全检查未通过: {reason}",
                return_code=-1,
                execution_time=0,
                timed_out=False,
                memory_exceeded=False,
                security_blocked=True,
                blocked_reason=reason
            )
        
        # 如果有测试用例，运行所有测试
        if test_cases:
            return self._run_with_test_cases(code, test_cases)
        
        # 单次运行
        return self._execute_code(code, stdin_input)
    
    def _run_with_test_cases(self, code: str, test_cases: List[Dict]) -> RunResult:
        """运行代码并测试所有用例"""
        results = []
        total_time = 0
        all_passed = True
        
        for i, tc in enumerate(test_cases):
            tc_input = tc.get('input', '')
            expected = tc.get('expected', tc.get('output', ''))
            
            result = self._execute_code(code, tc_input)
            total_time += result.execution_time
            
            actual_output = result.output.strip()
            expected_output = expected.strip()
            
            # 智能比较：处理input()提示文字的情况
            passed = self._smart_compare(actual_output, expected_output)
            if not passed:
                all_passed = False
            
            # 提取纯输出用于显示
            clean_output = self._extract_clean_output(actual_output)
            
            results.append({
                'case': i + 1,
                'input': tc_input,
                'expected': expected_output,
                'actual': actual_output,
                'actual_clean': clean_output,
                'passed': passed,
                'error': result.error if not result.success else None
            })
            
            # 如果执行出错（非输出不匹配），停止测试
            if not result.success and (result.timed_out or result.error):
                break
        
        # 构建输出
        output_lines = []
        for r in results:
            status = "✓ 通过" if r['passed'] else "✗ 未通过"
            output_lines.append(f"测试用例 {r['case']}: {status}")
            output_lines.append(f"  输入: {r['input'][:50]}{'...' if len(r['input']) > 50 else ''}")
            output_lines.append(f"  期望输出: {r['expected'][:50]}{'...' if len(r['expected']) > 50 else ''}")
            # 显示清理后的输出
            clean = r['actual_clean']
            output_lines.append(f"  实际输出: {clean[:50]}{'...' if len(clean) > 50 else ''}")
            if r['error']:
                output_lines.append(f"  错误: {r['error'][:100]}")
            output_lines.append("")
        
        passed_count = sum(1 for r in results if r['passed'])
        output_lines.append(f"总计: {passed_count}/{len(test_cases)} 测试通过")
        
        return RunResult(
            success=all_passed,
            output="\n".join(output_lines),
            error="" if all_passed else "部分测试用例未通过",
            return_code=0 if all_passed else 1,
            execution_time=total_time,
            timed_out=False,
            memory_exceeded=False,
            security_blocked=False,
            blocked_reason=""
        )
    
    def _smart_compare(self, actual: str, expected: str) -> bool:
        """
        智能比较实际输出和期望输出
        处理input()提示文字的情况
        """
        # 1. 精确匹配
        if actual == expected:
            return True
        
        # 2. 提取纯输出后比较
        clean_actual = self._extract_clean_output(actual)
        if clean_actual == expected:
            return True
        
        # 3. 检查期望输出是否在实际输出末尾
        if actual.endswith(expected):
            return True
        
        # 4. 按行比较（忽略input提示行）
        actual_lines = [self._extract_line_output(line) for line in actual.split('\n')]
        expected_lines = expected.split('\n')
        
        # 过滤空行后比较
        actual_lines = [l for l in actual_lines if l.strip()]
        expected_lines = [l for l in expected_lines if l.strip()]
        
        if actual_lines == expected_lines:
            return True
        
        return False
    
    def _extract_clean_output(self, output: str) -> str:
        """
        从输出中提取纯输出内容（去除input提示）
        """
        lines = output.split('\n')
        clean_lines = []
        
        for line in lines:
            clean_line = self._extract_line_output(line)
            if clean_line:  # 只保留非空行
                clean_lines.append(clean_line)
        
        return '\n'.join(clean_lines)
    
    def _extract_line_output(self, line: str) -> str:
        """
        从单行中提取纯输出（处理input提示）
        例如: "请输入成绩: 优秀" -> "优秀"
        """
        # 常见的input提示分隔符
        separators = [': ', '：', '> ', '>> ', '? ']
        
        for sep in separators:
            if sep in line:
                # 取最后一个分隔符后面的内容
                parts = line.rsplit(sep, 1)
                if len(parts) == 2:
                    # 如果分隔符后面有内容，返回它
                    after = parts[1].strip()
                    if after:
                        return after
        
        return line.strip()
    
    def _execute_code(self, code: str, stdin_input: str = "") -> RunResult:
        """实际执行代码"""
        import time
        import locale
        
        # 创建临时文件 - 确保使用UTF-8编码
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8') as f:
            # 在代码开头添加编码声明
            if not code.startswith('# -*- coding'):
                code = '# -*- coding: utf-8 -*-\n' + code
            f.write(code)
            temp_file = f.name
        
        try:
            start_time = time.time()
            
            # 设置环境变量确保UTF-8编码
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONDONTWRITEBYTECODE'] = '1'
            env['PYTHONUTF8'] = '1'  # Python 3.7+ UTF-8 mode
            env['LANG'] = 'en_US.UTF-8'
            env['LC_ALL'] = 'en_US.UTF-8'
            
            # 使用subprocess运行代码
            process = subprocess.Popen(
                [sys.executable, '-u', temp_file],  # -u 禁用缓冲
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )
            
            try:
                # 将输入编码为UTF-8字节
                input_bytes = stdin_input.encode('utf-8') if stdin_input else None
                stdout_bytes, stderr_bytes = process.communicate(
                    input=input_bytes,
                    timeout=self.timeout
                )
                execution_time = time.time() - start_time
                timed_out = False
                
                # 解码输出 - 尝试UTF-8，失败则用替换模式
                stdout = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
                stderr = stderr_bytes.decode('utf-8', errors='replace') if stderr_bytes else ""
                
            except subprocess.TimeoutExpired:
                process.kill()
                stdout_bytes, stderr_bytes = process.communicate()
                stdout = stdout_bytes.decode('utf-8', errors='replace') if stdout_bytes else ""
                execution_time = self.timeout
                timed_out = True
                return RunResult(
                    success=False,
                    output=stdout[:self.max_output] if stdout else "",
                    error=f"代码执行超时（超过{self.timeout}秒）",
                    return_code=-1,
                    execution_time=execution_time,
                    timed_out=True,
                    memory_exceeded=False,
                    security_blocked=False,
                    blocked_reason=""
                )
            
            # 检查是否有EOFError（用户未提供输入）
            if 'EOFError' in stderr and 'input' in code:
                return RunResult(
                    success=False,
                    output=stdout[:self.max_output] if stdout else "",
                    error='程序需要输入数据！请在「程序输入」区域填写输入内容后重新运行。',
                    return_code=process.returncode,
                    execution_time=execution_time,
                    timed_out=False,
                    memory_exceeded=False,
                    security_blocked=False,
                    blocked_reason=""
                )
            
            # 截断输出
            if len(stdout) > self.max_output:
                stdout = stdout[:self.max_output] + f"\n...(输出过长，已截断，共{len(stdout)}字符)"
            if len(stderr) > self.max_output:
                stderr = stderr[:self.max_output] + f"\n...(错误信息过长，已截断)"
            
            return RunResult(
                success=process.returncode == 0,
                output=stdout,
                error=stderr,
                return_code=process.returncode,
                execution_time=execution_time,
                timed_out=False,
                memory_exceeded=False,
                security_blocked=False,
                blocked_reason=""
            )
            
        except Exception as e:
            return RunResult(
                success=False,
                output="",
                error=f"执行错误: {str(e)}",
                return_code=-1,
                execution_time=0,
                timed_out=False,
                memory_exceeded=False,
                security_blocked=False,
                blocked_reason=""
            )
        finally:
            # 清理临时文件
            try:
                os.unlink(temp_file)
            except:
                pass


# 全局代码运行器实例
code_runner = CodeRunner()


def run_code(code: str, stdin_input: str = "", test_cases: List[Dict] = None) -> Dict[str, Any]:
    """
    便捷函数：运行代码并返回字典结果
    
    Args:
        code: Python代码
        stdin_input: 输入数据
        test_cases: 测试用例
        
    Returns:
        结果字典
    """
    result = code_runner.run(code, stdin_input, test_cases)
    return {
        'success': result.success,
        'output': result.output,
        'error': result.error,
        'return_code': result.return_code,
        'execution_time': round(result.execution_time, 3),
        'timed_out': result.timed_out,
        'memory_exceeded': result.memory_exceeded,
        'security_blocked': result.security_blocked,
        'blocked_reason': result.blocked_reason
    }

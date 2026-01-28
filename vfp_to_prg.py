import os
import re
import dbfread
import contextlib
import struct
from dbfread import DBF


def indent_text(text, levels=1):
    """Indents the given text block by the specified number of tabs."""
    if not text:
        return ""
    indent = "\t" * levels
    return "\n".join(
        [indent + line if line.strip() else line for line in text.splitlines()]
    )


class CustomVFPMemo:
    def __init__(self, file):
        self.file = file
        self.block_size = 512
        self.init_header()

    def init_header(self):
        try:
            self.file.seek(0)
            header = self.file.read(512)
            if len(header) >= 8:
                # VFP Memo Header
                # Offset 6-7 is block size (Big Endian)
                self.block_size = (header[6] << 8) | header[7]
                if self.block_size == 0:
                    self.block_size = 64
        except Exception:
            self.block_size = 64

    def __getitem__(self, index):
        if not isinstance(index, int) or index <= 0:
            return None

        try:
            offset = index * self.block_size
            self.file.seek(offset)

            # Read Block Header (8 bytes)
            # 00-03: Signature (Big Endian)
            # 04-07: Length (Big Endian)
            block_header = self.file.read(8)
            if len(block_header) < 8:
                return b""

            length = struct.unpack(">I", block_header[4:8])[0]

            # Read data
            data = self.file.read(length)

            # dbfread expects bytes and will decode it itself based on table encoding
            return data
        except Exception:
            return None

    def get(self, index):
        return self.__getitem__(index)


class VFPDBF(dbfread.DBF):
    def _get_memofilename(self):
        # Check files relative to the DBF file
        base, ext = os.path.splitext(self.filename)
        ext = ext.lower()

        candidates = []
        # Standard
        candidates.append(base + ".fpt")
        candidates.append(base + ".FPT")
        candidates.append(base + ".dbt")
        # VFP
        if ext == ".vcx":
            candidates.append(base + ".vct")
            candidates.append(base + ".VCT")
        elif ext == ".scx":
            candidates.append(base + ".sct")
            candidates.append(base + ".SCT")
        elif ext == ".pjx":
            candidates.append(base + ".pjt")
            candidates.append(base + ".PJT")

        candidates.append(base + ".vct")
        candidates.append(base + ".sct")

        for c in candidates:
            if os.path.exists(c):
                return c

        return super()._get_memofilename()

    @contextlib.contextmanager
    def _open_memofile(self):
        if self.memofilename:
            try:
                # Open strictly as binary
                f = open(self.memofilename, "rb")
            except OSError:
                yield None
                return

            try:
                # Use our robust CustomVFPMemo reader
                yield CustomVFPMemo(f)
            finally:
                f.close()
        else:
            yield None


class VFPClassExporter:
    def __init__(self, file_path):
        self.file_path = file_path
        self.code_lines = []
        self.current_class = None
        self.deferred_methods = []
        self.method_comments = {}

    def _fix_prop_line(self, line):
        # Fix RGB colors: Prop = 128,128,128 -> Prop = RGB(128,128,128)
        # Regex to match: Name = R,G,B (with optional whitespace)
        match = re.search(r"(=)\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*$", line)
        if match:
            # Check if property name suggests color? Optional but safer.
            # Original output shows it applied to ForeColor, BackColor, etc.
            # But relying on the value structure is probably enough as comma separated tuples are rare
            return f"{line[:match.start(1)]}= RGB({match.group(2)},{match.group(3)},{match.group(4)})"
        return line

    def export(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        # Use VFPDBF
        try:
            table = VFPDBF(
                self.file_path,
                load=True,
                encoding="cp1252",
                char_decode_errors="ignore",
            )
        except dbfread.exceptions.MissingMemoFile:
            print(
                f"Warning: Could not find memo file (.vct/.sct/.fpt) for {self.file_path}"
            )
            raise

        records = list(table)

        # Helper to safely get field values
        def get_val(rec, name):
            for key in rec:
                if key.lower() == name.lower():
                    val = rec[key]
                    return val.strip() if isinstance(val, str) else val
            return ""

        # Group records into classes
        class_groups = []
        current_group = None

        for record in records:
            parent = get_val(record, "Parent")

            if not parent:
                # Start of a new class definition
                current_group = {"header": record, "children": []}
                class_groups.append(current_group)
            else:
                # Child object belonging to the current class
                if current_group:
                    current_group["children"].append(record)

        # Sort classes by ObjName of the header record
        def get_obj_name(group):
            rec = group["header"]
            for key in rec:
                if key.lower() == "objname":
                    val = rec[key]
                    return val.lower() if isinstance(val, str) else ""
            return ""

        class_groups.sort(key=get_obj_name)

        # SCX Header Generation
        is_scx = self.file_path.lower().endswith(".scx")
        if is_scx:
            main_form_name = ""
            class_libs = set()

            for record in records:
                # Find main form name
                base_class = get_val(record, "BaseClass")
                parent = get_val(record, "Parent")
                if base_class and base_class.lower() == "form" and not parent:
                    main_form_name = get_val(record, "ObjName")

                # Collect Class Libraries
                c_loc = get_val(record, "ClassLoc")
                if c_loc:
                    class_libs.add(c_loc)

            if main_form_name:
                var_name = f"o{main_form_name.lower()}"
                self.code_lines.append(f"PUBLIC {var_name}")
                self.code_lines.append("")

                sorted_libs = sorted(list(class_libs))
                for lib in sorted_libs:
                    self.code_lines.append(f"SET CLASSLIB TO {lib} ADDITIVE")

                if sorted_libs:
                    self.code_lines.append("")

                self.code_lines.append(
                    f'{var_name}=NEWOBJECT("{main_form_name.lower()}")'
                )
                self.code_lines.append(f"{var_name}.Show")
                self.code_lines.append("RETURN")
                self.code_lines.append("")
                self.code_lines.append("")

        # Process sorted groups
        for group in class_groups:
            # Build parent hierarchy map
            # Map: lower_case_obj_name -> (original_name, lower_case_parent_name)
            obj_map = {}

            # Header name
            h_rec = group["header"]
            h_orig = get_val(h_rec, "ObjName")

            # Map children
            for child in group["children"]:
                c_name = get_val(child, "ObjName")
                p_name = get_val(child, "Parent")
                if c_name:
                    obj_map[c_name.lower()] = (c_name, p_name.lower() if p_name else "")

            # print(f"DEBUG MAP: {list(obj_map.keys())}")

            self.process_record(group["header"], obj_map=obj_map, root_name=h_orig)
            for child in group["children"]:
                self.process_record(child, obj_map=obj_map, root_name=h_orig)

        self.end_current_class()
        return "\n".join(self.code_lines)

    def process_record(self, record, obj_map=None, root_name=None):
        def get_field(name):
            for key in record:
                if key.lower() == name.lower():
                    val = record[key]
                    return val.strip() if isinstance(val, str) else val
            return ""

        obj_name = get_field("ObjName")
        parent = get_field("Parent")
        base_class = get_field("BaseClass")
        parent_class = get_field("Class")
        class_loc = get_field("ClassLoc")
        properties = get_field("Properties")
        methods = get_field("Methods")
        reserved3 = get_field("Reserved3")
        protected = get_field("Protected")
        platform = get_field("Platform")

        if platform and platform.upper() == "COMMENT":
            return

        if not parent:
            self.end_current_class()
            self.start_new_class(
                obj_name,
                parent_class,
                base_class,
                class_loc,
                properties,
                reserved3,
                protected,
                methods,
            )
        else:
            if self.current_class:
                # Calculate full path name
                full_name = obj_name
                path_parts = [obj_name]

                if obj_map is not None:
                    curr = obj_name.lower()
                    seen = {curr}

                    while True:
                        if curr not in obj_map:
                            break

                        _, p_lower = obj_map[curr]
                        if not p_lower:
                            break

                        # Check if parent is root
                        if root_name and p_lower == root_name.lower():
                            path_parts.append(root_name)
                            break

                        # Look up parent original name
                        if p_lower in obj_map:
                            orig_p, _ = obj_map[p_lower]
                            path_parts.append(orig_p)
                            curr = p_lower
                        else:
                            # Parent not in children map?
                            # If parent is root
                            if root_name and p_lower == root_name.lower():
                                path_parts.append(root_name)
                            break

                        if curr in seen:
                            break
                        seen.add(curr)

                # Decide full_name based on traversal results
                path_reaches_root = False
                if len(path_parts) > 1:
                    # check if the last part is indeed the root
                    if root_name and path_parts[-1].lower() == root_name.lower():
                        path_reaches_root = True

                if path_reaches_root:
                    full_name = ".".join(reversed(path_parts))
                else:
                    # Traversal failed or didn't reach root.
                    # Use raw Parent field if it provides better info.
                    if parent:
                        # If parent looks like a path or is the root
                        if "." in parent or (
                            root_name and parent.lower() == root_name.lower()
                        ):
                            # Adjust parent case if it matches root
                            p_str = parent
                            if root_name and parent.lower() == root_name.lower():
                                p_str = root_name

                            full_name = f"{p_str}.{obj_name}"

                # Fallback to BaseClass if ParentClass is empty
                actual_class = parent_class if parent_class else base_class

                self.add_child_object(
                    full_name, actual_class, class_loc, properties, methods
                )

    def _indent_method_body(self, methods_text, method_comments=None):
        if not methods_text:
            return ""

        if method_comments is None:
            method_comments = {}

        lines = methods_text.splitlines()
        output_lines = []
        body_buffer = []

        # Capture method name in group 1
        # Supports: PROCEDURE Name, FUNCTION Name, PROTECTED PROCEDURE Name, etc.
        # Updated to support Object.Method syntax (dots in name)
        start_re = re.compile(
            r"^\s*(?:HIDDEN\s+|PROTECTED\s+)?(?:PROCEDURE|FUNCTION)\s+([a-zA-Z0-9_.]+)",
            re.IGNORECASE,
        )
        end_re = re.compile(r"^\s*(?:ENDPROC|ENDFUNC)", re.IGNORECASE)

        in_method = False

        for line in lines:
            stripped = line.strip()
            match = start_re.match(stripped)

            if match:
                # Flush any pending body if malformed (missing ENDPROC)
                if in_method and body_buffer:
                    for b in body_buffer:
                        output_lines.append(f"\t{b}" if b.strip() else "")
                    body_buffer = []

                # Insert Comment if exists
                method_name = match.group(1).lower()
                if method_name in method_comments:
                    desc = method_comments[method_name]
                    # Add blank line before comment if needed?
                    output_lines.append(f"*-- {desc}")

                output_lines.append(line.lstrip())
                in_method = True

            elif end_re.match(stripped):
                # Flush body: trim trailing empty lines
                while body_buffer and not body_buffer[-1].strip():
                    body_buffer.pop()

                for b in body_buffer:
                    # Preserve relative indent but add one tab
                    output_lines.append(f"\t{b}" if b.strip() else "")
                body_buffer = []

                output_lines.append(line.lstrip())
                in_method = False

                # Add 2 blank lines after ENDPROC
                output_lines.append("")
                output_lines.append("")

            else:
                if in_method:
                    body_buffer.append(line)
                else:
                    # Content outside method (e.g. initial comments)
                    # Strip leading/trailing blank lines?
                    if stripped:
                        output_lines.append(line)

        # Clean up trailing empty lines at end of file
        while output_lines and not output_lines[-1].strip():
            output_lines.pop()

        return "\n".join(output_lines)

    def start_new_class(
        self, name, parent_class, base_class, properties, reserved3, protected, methods
    ):
        self.current_class = name
        self.deferred_methods = []
        # Reset method comments for the new class
        self.method_comments = {}

        self.code_lines.append("**************************************************")

        # Check file extension to determine label
        ext = os.path.splitext(self.file_path)[1].lower()
        if ext == ".scx":
            # "Form:" plus 9 spaces = 14 chars length for alignment
            self.code_lines.append(f"*-- Form:         {name}")
        else:
            # "Class:" plus 8 spaces = 14 chars length
            self.code_lines.append(f"*-- Class:        {name}")

        self.code_lines.append(f"*-- ParentClass:  {parent_class}")
        self.code_lines.append(f"*-- BaseClass:    {base_class}")
        self.code_lines.append("**************************************************")
        self.code_lines.append("")

        def_line = f"DEFINE CLASS {name} AS {parent_class}"
        self.code_lines.append(def_line)

        # Parse Protected/Hidden lists
        protected_list = []
        hidden_list = []
        if protected:
            for p in protected.splitlines():
                p = p.strip()
                if not p:
                    continue
                if "^" in p:
                    hidden_list.append(p.replace("^", ""))
                else:
                    protected_list.append(p)

        # Prepare properties extraction
        names = {}

        # Parse Reserved3 for property/array definitions AND comments
        prop_comments = {}

        def_lines = []
        if reserved3:
            for line in reserved3.splitlines():
                line = line.strip()
                if not line:
                    continue

                # Method Comment: Starts with *
                # Format: *MethodName Description
                if line.startswith("*"):
                    content = line[1:].strip()
                    # split on first space
                    parts = content.split(" ", 1)
                    if len(parts) == 2:
                        m_name, m_desc = parts
                        self.method_comments[m_name.lower()] = m_desc.strip()
                    continue

                if line.startswith("^"):
                    # Array definition
                    raw = line[1:]
                    # VFP Browser logic: replace ( with [, ) with ], and ,0] with ]
                    raw = raw.replace("(", "[").replace(")", "]").replace(",0]", "]")

                    # Split on space max 1
                    parts = raw.split(" ", 1)
                    arr_def = parts[0]
                    arr_desc = parts[1] if len(parts) > 1 else ""

                    def_str = f"DIMENSION {arr_def}"
                    mem_name = arr_def.split("[")[0].lower()
                    names[mem_name] = True

                    if mem_name in hidden_list:
                        def_str = f"HIDDEN {def_str}"
                    elif mem_name in protected_list:
                        def_str = f"PROTECTED {def_str}"

                    if arr_desc:
                        prop_comments[mem_name] = arr_desc

                    def_lines.append(
                        (mem_name, def_str)
                    )  # Store tuple to match sorting/grouping later?
                else:
                    # Property definition: PropName Description
                    parts = line.split(" ", 1)
                    mem_name = parts[0].lower()
                    mem_desc = parts[1] if len(parts) > 1 else ""

                    # Only add if not already seen (deduplicate)
                    if mem_name not in names:
                        names[mem_name] = True
                        def_str = f"{mem_name} = .F."
                        if mem_name in hidden_list:
                            def_str = f"HIDDEN {def_str}"
                        elif mem_name in protected_list:
                            def_str = f"PROTECTED {def_str}"

                        if mem_desc:
                            prop_comments[mem_name] = mem_desc

                        def_lines.append((mem_name, def_str))

        # 2. Process explicit Properties (values)
        prop_lines = []
        if properties:
            for line in properties.splitlines():
                if not line.strip():
                    continue
                prop_lines.append(self._fix_prop_line(line.strip()))

        # Combine Output

        # 1. Name
        name_prop = None
        final_props = []
        for p in prop_lines:
            if p.lower().startswith("name ="):
                name_prop = p
            else:
                final_props.append(p)

        if name_prop:
            self.code_lines.append(indent_text(name_prop, 1))

        # 2. Definitions (Reserved3)
        for mem_name, d_str in def_lines:
            if mem_name in prop_comments:
                self.code_lines.append("")
                self.code_lines.append(indent_text(f"*-- {prop_comments[mem_name]}", 1))
            self.code_lines.append(indent_text(d_str, 1))

        # 3. Properties
        for p in final_props:
            # check if we have a comment for this property?
            p_name = p.split("=")[0].strip().lower()
            if p_name in prop_comments:
                pass

            self.code_lines.append(indent_text(p, 1))

        if methods:
            # Defer method printing to the end (after all ADD OBJECTs for child controls)
            self.deferred_methods.append(methods)

    def end_current_class(self):
        if self.current_class:
            if self.deferred_methods:
                self.code_lines.append("")
                # Join all deferred methods with newlines
                all_methods = "\n\n".join(self.deferred_methods)
                # Use _indent_method_body to handle indentation properly
                # Pass collected method comments
                indented_methods = self._indent_method_body(
                    all_methods, self.method_comments
                )
                self.code_lines.append(indent_text(indented_methods, 1))

            self.code_lines.append("ENDDEFINE")
            self.code_lines.append("")
            self.current_class = None

    def add_child_object(self, name, parent_class, class_loc, properties, methods):
        self.code_lines.append("")
        add_cmd = f"ADD OBJECT {name} AS {parent_class}"
        if class_loc:
            add_cmd += f' OF "{class_loc}"'

        if properties:
            add_cmd += " WITH ;"
            self.code_lines.append(indent_text(add_cmd, 1))
            props_list = [
                self._fix_prop_line(p.strip())
                for p in properties.splitlines()
                if p.strip()
            ]
            props_formatted = ", ;\n".join(props_list)
            self.code_lines.append(indent_text(props_formatted, 2))
        else:
            self.code_lines.append(indent_text(add_cmd, 1))

        if methods:
            pattern = re.compile(r"(PROCEDURE\s+)([a-zA-Z0-9_]+)", re.IGNORECASE)
            # Use the relative name for method definitions, not the full ADD OBJECT path?
            # If name passed here is "Container.Control", we likely want "Control"
            # But if name is "class.control", we want "control".

            # The rule seems to be: exclude the Root Class Name from the procedure prefix.

            def replacer(match):
                # name is e.g. "Root.Container.Control"
                # We want "Container.Control"

                # Heuristic: if name has dots, strip the first segment if it matches current_class?
                # self.current_class is set.

                prefix = name
                if self.current_class and prefix.lower().startswith(
                    self.current_class.lower() + "."
                ):
                    prefix = prefix[len(self.current_class) + 1:]

                # Strip parent container path because VFP exports only the immediate object name?
                # Looking at original VFP exports:
                # PROCEDURE txtpriority.Valid
                # But the object is ADD OBJECT Root.Container.Control.txtPriority
                # This implies we ONLY want the leaf name + method name

                if "." in prefix:
                    prefix = prefix.split(".")[-1]

                return f"{match.group(1)}{prefix}.{match.group(2)}"

            fixed_methods = pattern.sub(replacer, methods)
            self.deferred_methods.append(fixed_methods)


class ProjectExporter:
    def __init__(self, pjx_path, output_dir):
        self.pjx_path = pjx_path
        self.output_dir = output_dir
        self.project_root = os.path.dirname(os.path.abspath(pjx_path))

    def export(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        print(f"Opening project: {self.pjx_path}")
        try:
            table = VFPDBF(
                self.pjx_path,
                load=True,
                encoding="cp1252",
                char_decode_errors="ignore",
            )
        except dbfread.exceptions.MissingMemoFile:
            print(
                "Warning: Memo file missing for PJX. Attempting to read without memo."
            )
            table = DBF(
                self.pjx_path, load=True, encoding="cp1252", char_decode_errors="ignore"
            )

        print(f"Found {len(table)} records.")

        processed_count = 0
        for record in table:
            name = self._get_val(record, "Name")
            type_code = self._get_val(record, "Type")

            # Skip empty names or project header (Type 'H')
            if not name or type_code == "H":
                continue

            # Clean name (remove nulls etc)
            name = name.strip("\x00").strip()

            src_path = self._resolve_path(name)
            if not src_path:
                print(f"MISSING: {name} (Original: {record.get('Name')})")
                continue

            # Determine output relative path
            try:
                rel_path = os.path.relpath(src_path, self.project_root)
            except ValueError:
                rel_path = os.path.basename(src_path)

            if rel_path.startswith(".."):
                dest_rel = os.path.basename(src_path)
            else:
                dest_rel = rel_path

            dest_full_path = os.path.join(self.output_dir, dest_rel)

            if type_code.upper() in ["K", "s"]:  # Form (.scx)
                self._convert_and_write(src_path, dest_full_path + ".prg")
            elif type_code.upper() in ["V", "c"]:  # Class (.vcx)
                self._convert_and_write(src_path, dest_full_path + ".prg")
            elif type_code.upper() == "P":  # Program (.prg)
                self._copy_text(src_path, dest_full_path)

            processed_count += 1

        print(f"Processed {processed_count} files.")

    def _get_val(self, rec, name):
        for key in rec:
            if key.lower() == name.lower():
                val = rec[key]
                return val.strip() if isinstance(val, str) else val
        return ""

    def _resolve_path(self, name):
        # 1. Check absolute
        if os.path.exists(name):
            return os.path.abspath(name)

        # 2. Check relative to project root
        candidate = os.path.join(self.project_root, name)
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

        # 3. Check basename in project root (flattened find)
        basename = os.path.basename(name)
        candidate = os.path.join(self.project_root, basename)
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

        return None

    def _convert_and_write(self, src, dest):
        if not os.path.exists(src):
            return

        print(f"Converting: {os.path.basename(src)}")
        os.makedirs(os.path.dirname(dest), exist_ok=True)

        try:
            exporter = VFPClassExporter(src)
            content = exporter.export()
            with open(dest, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            print(f"ERROR convert {src}: {e}")

    def _copy_text(self, src, dest):
        if not os.path.exists(src):
            return

        print(f"Copying:    {os.path.basename(src)}")
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        try:
            with open(src, "rb") as f:
                raw = f.read()
            text = raw.decode("cp1252", errors="replace")
            with open(dest, "w", encoding="utf-8") as f:
                f.write(text)
        except Exception as e:
            print(f"ERROR copy {src}: {e}")


if __name__ == "__main__":
    import sys
    import traceback

    if len(sys.argv) < 2:
        print("Usage: python vfp_to_prg.py <path_to_vcx_or_scx_or_pjx> [output_dir]")
    else:
        fpath = sys.argv[1]
        ext = os.path.splitext(fpath)[1].lower()

        try:
            if ext == ".pjx":
                if len(sys.argv) >= 3:
                    out_dir = sys.argv[2]
                else:
                    # Default to folder named after project + "_export"
                    base = os.path.splitext(os.path.basename(fpath))[0]
                    out_dir = os.path.join(os.path.dirname(fpath), base + "_export")

                exporter = ProjectExporter(fpath, out_dir)
                exporter.export()
                print(f"Export completed to: {out_dir}")

            else:
                exporter = VFPClassExporter(fpath)
                print(exporter.export())
        except Exception as e:
            traceback.print_exc()
            print(f"Error: {e}")

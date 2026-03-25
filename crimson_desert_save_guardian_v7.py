#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import filecmp
import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import customtkinter as ctk
from tkinter import filedialog, messagebox
from tkinter import ttk


APP_TITLE = "Crimson Desert Save Guardian"
CONFIG_FILE = "save_guardian_config.json"
MANIFEST_FILE = "backup_manifest.json"

DEFAULT_SOURCE = r"C:\Users\Ygor\AppData\Local\Pearl Abyss\CD\save"
DEFAULT_BACKUP_DIR = str(Path.home() / "CrimsonDesert_SaveBackups")
DEFAULT_INTERVAL_MINUTES = 10
DEFAULT_MAX_VERSIONS = 10


@dataclass
class Config:
    source_dir: str = DEFAULT_SOURCE
    backup_dir: str = DEFAULT_BACKUP_DIR
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES
    max_versions: int = DEFAULT_MAX_VERSIONS
    backup_on_start: bool = True
    only_if_changed: bool = True
    language: str = "pt-BR"


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{num_bytes} B"


def folder_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def iter_files(path: Path):
    if not path.exists():
        return []
    return sorted([p for p in path.rglob("*") if p.is_file()], key=lambda x: str(x).lower())


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def directories_equal(dir1: Path, dir2: Path) -> bool:
    if not dir1.exists() or not dir2.exists():
        return False

    files1 = [p.relative_to(dir1) for p in iter_files(dir1) if p.name != MANIFEST_FILE]
    files2 = [p.relative_to(dir2) for p in iter_files(dir2) if p.name != MANIFEST_FILE]

    if files1 != files2:
        return False

    for rel in files1:
        f1 = dir1 / rel
        f2 = dir2 / rel
        try:
            if f1.stat().st_size != f2.stat().st_size:
                return False
            if not filecmp.cmp(f1, f2, shallow=False):
                return False
        except Exception:
            return False
    return True




def detect_default_save_path() -> Optional[Path]:
    userprofile = os.environ.get("USERPROFILE") or str(Path.home())
    candidates = [
        Path(userprofile) / "AppData" / "Local" / "Pearl Abyss" / "CD" / "save",
        Path(userprofile) / "AppData" / "Local" / "Pearl Abyss" / "CrimsonDesert" / "save",
        Path.home() / "AppData" / "Local" / "Pearl Abyss" / "CD" / "save",
        Path.home() / "AppData" / "Local" / "Pearl Abyss" / "CrimsonDesert" / "save",
    ]
    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_dir():
                return candidate
        except Exception:
            pass
    return None

class BackupService:
    def __init__(self, config_path: Path, ui):
        self.config_path = config_path
        self.ui = ui
        self.config = self.load_config()

        self.running = False
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

        self.last_backup_time: Optional[str] = None
        self.last_backup_path: Optional[str] = None
        self.last_error: Optional[str] = None
        self.next_backup_epoch: Optional[float] = None

    def log(self, msg: str):
        self.ui.log(f"[{now_text()}] {msg}")

    def load_config(self) -> Config:
        if self.config_path.exists():
            try:
                data = json.loads(self.config_path.read_text(encoding="utf-8"))
                return Config(**data)
            except Exception:
                pass
        cfg = Config()
        self.save_config(cfg)
        return cfg

    def save_config(self, cfg: Optional[Config] = None):
        cfg = cfg or self.config
        self.config_path.write_text(
            json.dumps(asdict(cfg), ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    @property
    def source_path(self) -> Path:
        return Path(self.config.source_dir)

    @property
    def backup_root(self) -> Path:
        return Path(self.config.backup_dir)

    def validate(self):
        if not self.source_path.exists():
            raise FileNotFoundError(f"Pasta de save não encontrada:\n{self.source_path}")
        if not self.source_path.is_dir():
            raise NotADirectoryError(f"O caminho informado não é uma pasta:\n{self.source_path}")
        self.backup_root.mkdir(parents=True, exist_ok=True)

    def list_backups(self):
        if not self.backup_root.exists():
            return []
        items = [p for p in self.backup_root.iterdir() if p.is_dir() and p.name.startswith("backup_")]
        items.sort(key=lambda p: p.name, reverse=True)
        return items

    def latest_backup(self) -> Optional[Path]:
        backups = self.list_backups()
        return backups[0] if backups else None

    def has_changes_since_last_backup(self) -> bool:
        latest = self.latest_backup()
        if latest is None:
            return True
        return not directories_equal(self.source_path, latest)

    def build_manifest(self, backup_dir: Path):
        files = []
        for f in iter_files(backup_dir):
            if f.name == MANIFEST_FILE:
                continue
            rel = str(f.relative_to(backup_dir)).replace("\\", "/")
            files.append({
                "path": rel,
                "size": f.stat().st_size,
                "sha256": sha256_file(f),
            })

        manifest = {
            "created_at": now_text(),
            "source_dir": str(self.source_path),
            "backup_dir": str(backup_dir),
            "total_files": len(files),
            "files": files,
        }
        (backup_dir / MANIFEST_FILE).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    def verify_backup_integrity(self, backup_dir: Path) -> tuple[bool, str]:
        manifest_path = backup_dir / MANIFEST_FILE
        if not manifest_path.exists():
            return False, "manifesto ausente"

        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            return False, f"manifesto inválido: {exc}"

        for entry in manifest.get("files", []):
            f = backup_dir / entry["path"]
            if not f.exists():
                return False, f"arquivo ausente: {entry['path']}"
            try:
                if f.stat().st_size != entry["size"]:
                    return False, f"tamanho divergente: {entry['path']}"
                if sha256_file(f) != entry["sha256"]:
                    return False, f"checksum divergente: {entry['path']}"
            except Exception as exc:
                return False, f"erro ao validar {entry['path']}: {exc}"

        return True, "ok"

    def latest_healthy_backup(self) -> Optional[Path]:
        for backup in self.list_backups():
            ok, _ = self.verify_backup_integrity(backup)
            if ok:
                return backup
        return None

    def cleanup_old_backups(self):
        backups = self.list_backups()
        keep = max(1, int(self.config.max_versions))
        for old in backups[keep:]:
            try:
                shutil.rmtree(old)
                self.log(f"Backup antigo removido: {old.name}")
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Erro ao remover backup antigo {old.name}: {exc}")

    def create_backup(self, manual=False):
        self.validate()

        if self.config.only_if_changed and not manual:
            if not self.has_changes_since_last_backup():
                self.log("Nenhuma alteração detectada desde o último backup. Backup automático ignorado.")
                return None

        target = self.backup_root / f"backup_{stamp()}"
        shutil.copytree(self.source_path, target)
        self.build_manifest(target)

        ok, reason = self.verify_backup_integrity(target)
        if not ok:
            shutil.rmtree(target, ignore_errors=True)
            raise RuntimeError(f"Falha na validação de integridade do backup: {reason}")

        self.last_backup_time = now_text()
        self.last_backup_path = str(target)
        self.last_error = None
        self.cleanup_old_backups()

        self.log(f"Backup {'manual' if manual else 'automático'} criado e validado: {target}")
        self.ui.refresh_all()
        return target

    def restore_backup(self, selected_backup: Path):
        self.validate()

        if not selected_backup.exists():
            raise FileNotFoundError("O backup selecionado não existe mais.")

        ok, reason = self.verify_backup_integrity(selected_backup)
        if not ok:
            raise RuntimeError(f"O backup selecionado não está íntegro: {reason}")

        safety = self.backup_root / f"pre_restore_{stamp()}"
        shutil.copytree(self.source_path, safety)
        self.build_manifest(safety)
        self.log(f"Backup de segurança criado antes do restore: {safety}")

        temp = self.backup_root / f"tmp_restore_{stamp()}"
        shutil.copytree(selected_backup, temp)

        for item in self.source_path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        for item in temp.iterdir():
            if item.name == MANIFEST_FILE:
                continue
            target = self.source_path / item.name
            if item.is_dir():
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)

        shutil.rmtree(temp, ignore_errors=True)
        self.last_error = None
        self.log(f"Restore concluído com sucesso: {selected_backup.name}")
        self.ui.refresh_all()

    def restore_latest_healthy_backup(self) -> Path:
        backup = self.latest_healthy_backup()
        if backup is None:
            raise RuntimeError("Nenhum backup saudável foi encontrado.")
        self.restore_backup(backup)
        return backup

    def start(self):
        if self.running:
            self.log("Monitor já está ativo.")
            return

        self.validate()
        self.stop_event.clear()
        self.running = True

        if self.config.backup_on_start:
            try:
                self.create_backup(manual=False)
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Erro ao criar backup inicial: {exc}")

        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()
        self.log("Monitor iniciado.")
        self.ui.refresh_all()

    def stop(self):
        if not self.running:
            self.log("Monitor já está parado.")
            return
        self.stop_event.set()
        self.running = False
        self.next_backup_epoch = None
        self.log("Monitor parado.")
        self.ui.refresh_all()

    def _worker(self):
        while not self.stop_event.is_set():
            self.next_backup_epoch = time.time() + max(1, int(self.config.interval_minutes)) * 60
            while not self.stop_event.is_set() and time.time() < self.next_backup_epoch:
                self.ui.update_countdown(max(0, int(self.next_backup_epoch - time.time())))
                time.sleep(1)

            if self.stop_event.is_set():
                break

            try:
                self.create_backup(manual=False)
            except Exception as exc:
                self.last_error = str(exc)
                self.log(f"Erro no backup automático: {exc}")
                self.ui.refresh_all()

        self.running = False
        self.ui.update_countdown(None)
        self.ui.refresh_all()








LANGUAGE_OPTIONS = {
    "pt-BR": "Português (Brasil)",
    "en-US": "English (US)",
}

TRANSLATIONS = {
    "pt-BR": {
        "app_title": "Crimson Desert Save Guardian",
        "configurations": "Configurações",
        "save_folder": "Pasta do save",
        "backup_folder": "Pasta de backup",
        "interval_min": "Intervalo (min)",
        "max_backups": "Máx. backups",
        "backup_on_start": "Criar backup ao iniciar monitor",
        "only_if_changed": "Fazer backup apenas se o save mudou",
        "language": "Idioma",
        "save_settings": "Salvar configurações",
        "actions": "Ações",
        "start_auto_backup": "Iniciar Backup Automático",
        "stop_auto_backup": "Parar Backup Automático",
        "backup_now": "Backup agora",
        "restore_selected": "Restaurar selecionado",
        "restore_latest_healthy": "Restaurar último backup saudável",
        "validate_selected": "Validar backup selecionado",
        "delete_selected": "Excluir backup selecionado",
        "open_save_folder": "Abrir pasta de saves",
        "open_backup_folder": "Abrir pasta de backups",
        "status": "Status",
        "next_backup": "Próximo backup",
        "backups": "Backups",
        "save_size": "Tamanho save",
        "latest_healthy": "Último saudável",
        "latest_error": "Último erro",
        "latest_backup": "Último backup",
        "search_backup": "Buscar backup",
        "search_placeholder": "Nome, data, caminho...",
        "only_healthy": "Somente íntegros",
        "refresh_list": "Atualizar lista",
        "available_backups": "Backups disponíveis",
        "log": "Log",
        "ready": "Pronto.",
        "showing_backups": "Exibindo {count} backup(s) | Ordenação: {column} {order}",
        "sort_asc": "asc",
        "sort_desc": "desc",
        "active": "ATIVO",
        "stopped": "PARADO",
        "missing_folder": "pasta ausente",
        "none": "nenhum",
        "yes": "Sim",
        "no": "Não",
        "warning": "Aviso",
        "error": "Erro",
        "success": "Sucesso",
        "info": "Informação",
        "integrity": "Integridade",
        "quick_action": "Ação rápida",
        "quick_action_backup": "Backup selecionado:\n{backup}",
        "restore": "Restaurar",
        "validate": "Validar",
        "close": "Fechar",
        "ok": "OK",
        "confirm": "Confirmar",
        "cancel": "Cancelar",
        "confirm_restore_title": "Confirmar restore",
        "confirm_restore_msg": "Isso vai substituir o save atual.\n\nAntes disso, será criado um backup de segurança.\n\nDeseja continuar?",
        "confirm_restore_healthy_title": "Restaurar último backup saudável",
        "confirm_restore_healthy_msg": "O aplicativo vai localizar o backup íntegro mais recente e restaurá-lo.\n\nDeseja continuar?",
        "confirm_delete_title": "Excluir backup",
        "confirm_delete_msg": "Deseja excluir o backup abaixo?\n\n{backup}",
        "select_backup": "Selecione um backup na lista.",
        "backup_created": "Backup criado com sucesso.",
        "no_changes": "Nenhuma alteração detectada.",
        "backup_integrity_ok": "Backup íntegro:\n{backup}",
        "backup_integrity_bad": "Backup com problema:\n{reason}",
        "restore_success": "Restore concluído com sucesso.",
        "restore_success_using": "Restore concluído usando:\n{backup}",
        "delete_success_log": "Backup removido manualmente: {backup}",
        "config_saved_log": "Configurações salvas.",
        "folder_not_found": "Pasta não encontrada:\n{path}",
        "build_name": "Nome",
        "build_date": "Data",
        "build_size": "Tamanho",
        "build_healthy": "Íntegro",
        "build_path": "Caminho",
        "export_log": "Exportar log",
        "clear_log": "Limpar log",
        "log_exported": "Log exportado com sucesso.",
        "log_cleared": "Log limpo.",
        "relative_now": "agora",
        "minutes_ago": "há {n} min",
        "hours_ago": "há {n} h",
        "days_ago": "há {n} d",
        "language_changed": "Idioma alterado para {language}.",
        "select_save_folder": "Selecione a pasta do save",
        "select_backup_folder": "Selecione a pasta dos backups",
        "save_log_dialog_title": "Salvar log",
        "log_file_name": "save_guardian_log.txt",
        "auto_detect": "Auto Detectar",
        "save_detected": "Pasta de save detectada automaticamente.",
        "save_not_detected": "Não foi possível detectar automaticamente a pasta de save.",
        "save_detected_using": "Save detectado automaticamente: {path}",
    },
    "en-US": {
        "app_title": "Crimson Desert Save Guardian",
        "configurations": "Settings",
        "save_folder": "Save folder",
        "backup_folder": "Backup folder",
        "interval_min": "Interval (min)",
        "max_backups": "Max backups",
        "backup_on_start": "Create backup when monitor starts",
        "only_if_changed": "Backup only if save changed",
        "language": "Language",
        "save_settings": "Save settings",
        "actions": "Actions",
        "start_auto_backup": "Start Automatic Backup",
        "stop_auto_backup": "Stop Automatic Backup",
        "backup_now": "Backup now",
        "restore_selected": "Restore selected",
        "restore_latest_healthy": "Restore latest healthy backup",
        "validate_selected": "Validate selected backup",
        "delete_selected": "Delete selected backup",
        "open_save_folder": "Open save folder",
        "open_backup_folder": "Open backup folder",
        "status": "Status",
        "next_backup": "Next backup",
        "backups": "Backups",
        "save_size": "Save size",
        "latest_healthy": "Latest healthy",
        "latest_error": "Latest error",
        "latest_backup": "Latest backup",
        "search_backup": "Search backup",
        "search_placeholder": "Name, date, path...",
        "only_healthy": "Healthy only",
        "refresh_list": "Refresh list",
        "available_backups": "Available backups",
        "log": "Log",
        "ready": "Ready.",
        "showing_backups": "Showing {count} backup(s) | Sort: {column} {order}",
        "sort_asc": "asc",
        "sort_desc": "desc",
        "active": "ACTIVE",
        "stopped": "STOPPED",
        "missing_folder": "folder missing",
        "none": "none",
        "yes": "Yes",
        "no": "No",
        "warning": "Warning",
        "error": "Error",
        "success": "Success",
        "info": "Info",
        "integrity": "Integrity",
        "quick_action": "Quick action",
        "quick_action_backup": "Selected backup:\n{backup}",
        "restore": "Restore",
        "validate": "Validate",
        "close": "Close",
        "ok": "OK",
        "confirm": "Confirm",
        "cancel": "Cancel",
        "confirm_restore_title": "Confirm restore",
        "confirm_restore_msg": "This will replace the current save.\n\nA safety backup will be created before restoring.\n\nDo you want to continue?",
        "confirm_restore_healthy_title": "Restore latest healthy backup",
        "confirm_restore_healthy_msg": "The app will locate the most recent healthy backup and restore it.\n\nDo you want to continue?",
        "confirm_delete_title": "Delete backup",
        "confirm_delete_msg": "Do you want to delete the backup below?\n\n{backup}",
        "select_backup": "Select a backup from the list.",
        "backup_created": "Backup created successfully.",
        "no_changes": "No changes detected.",
        "backup_integrity_ok": "Healthy backup:\n{backup}",
        "backup_integrity_bad": "Backup issue detected:\n{reason}",
        "restore_success": "Restore completed successfully.",
        "restore_success_using": "Restore completed using:\n{backup}",
        "delete_success_log": "Backup manually removed: {backup}",
        "config_saved_log": "Settings saved.",
        "folder_not_found": "Folder not found:\n{path}",
        "build_name": "Name",
        "build_date": "Date",
        "build_size": "Size",
        "build_healthy": "Healthy",
        "build_path": "Path",
        "export_log": "Export log",
        "clear_log": "Clear log",
        "log_exported": "Log exported successfully.",
        "log_cleared": "Log cleared.",
        "relative_now": "now",
        "minutes_ago": "{n} min ago",
        "hours_ago": "{n} h ago",
        "days_ago": "{n} d ago",
        "language_changed": "Language changed to {language}.",
        "select_save_folder": "Select the save folder",
        "select_backup_folder": "Select the backup folder",
        "save_log_dialog_title": "Save log",
        "log_file_name": "save_guardian_log.txt",
        "auto_detect": "Auto Detect",
        "save_detected": "Save folder detected automatically.",
        "save_not_detected": "Could not detect the save folder automatically.",
        "save_detected_using": "Save detected automatically: {path}",
    },
}


def human_relative_time(dt: datetime, lang: str) -> str:
    delta = datetime.now() - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return TRANSLATIONS[lang]["relative_now"]
    minutes = seconds // 60
    if minutes < 60:
        return TRANSLATIONS[lang]["minutes_ago"].format(n=minutes)
    hours = minutes // 60
    if hours < 24:
        return TRANSLATIONS[lang]["hours_ago"].format(n=hours)
    days = hours // 24
    return TRANSLATIONS[lang]["days_ago"].format(n=days)

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.app_dir = Path(__file__).resolve().parent
        self.service = BackupService(self.app_dir / CONFIG_FILE, self)
        self.lang = self.service.config.language if self.service.config.language in LANGUAGE_OPTIONS else "pt-BR"

        self.title(self.tr("app_title"))
        self.geometry("1420x900")
        self.minsize(1280, 820)

        self.source_var = ctk.StringVar()
        self.backup_var = ctk.StringVar()
        self.interval_var = ctk.StringVar()
        self.max_var = ctk.StringVar()
        self.backup_on_start_var = ctk.BooleanVar()
        self.only_if_changed_var = ctk.BooleanVar()
        self.search_var = ctk.StringVar()
        self.only_healthy_var = ctk.BooleanVar(value=False)
        self.language_name_var = ctk.StringVar(value=LANGUAGE_OPTIONS[self.lang])
        self.countdown_var = ctk.StringVar(value="-")
        self.status_var = ctk.StringVar(value=self.tr("stopped"))
        self.last_backup_var = ctk.StringVar(value="-")
        self.error_var = ctk.StringVar(value=self.tr("none"))
        self.backup_count_var = ctk.StringVar(value="0")
        self.save_size_var = ctk.StringVar(value="-")
        self.healthy_backup_var = ctk.StringVar(value="-")
        self.footer_var = ctk.StringVar(value=self.tr("ready"))

        self.sort_column = "date"
        self.sort_desc = True

        self._build_ui()
        self.load_config_to_ui()
        self.auto_detect_save_path(silent=True)
        self.apply_language()
        self.refresh_all()

    def tr(self, key: str, **kwargs) -> str:
        text = TRANSLATIONS.get(self.lang, TRANSLATIONS["pt-BR"]).get(key, key)
        return text.format(**kwargs) if kwargs else text

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=0)

        self.tree_style = ttk.Style()
        try:
            self.tree_style.theme_use("default")
        except Exception:
            pass
        self.tree_style.configure(
            "Guardian.Treeview",
            background="#0f172a",
            foreground="#f8fafc",
            fieldbackground="#0f172a",
            rowheight=31,
            borderwidth=0,
        )
        self.tree_style.map(
            "Guardian.Treeview",
            background=[("selected", "#2563eb")],
            foreground=[("selected", "#ffffff")]
        )
        self.tree_style.configure(
            "Guardian.Treeview.Heading",
            background="#111827",
            foreground="#ffffff",
            relief="flat",
            font=("Segoe UI", 10, "bold"),
        )

        header = ctk.CTkFrame(self, corner_radius=20, fg_color=("#0b3b6f", "#0a1a38"), height=86)
        header.grid(row=0, column=0, padx=16, pady=16, sticky="ew")
        header.grid_columnconfigure(0, weight=1)
        header.grid_propagate(False)

        self.header_title = ctk.CTkLabel(header, text="", font=ctk.CTkFont(size=29, weight="bold"))
        self.header_title.grid(row=0, column=0, padx=18, pady=(18, 18), sticky="w")

        content = ctk.CTkFrame(self, fg_color="transparent")
        content.grid(row=1, column=0, padx=16, pady=(0, 10), sticky="nsew")
        content.grid_columnconfigure(0, weight=0)
        content.grid_columnconfigure(1, weight=1)
        content.grid_rowconfigure(0, weight=1)

        left_outer = ctk.CTkFrame(content, width=400, corner_radius=20, fg_color=("#20232a", "#23262d"))
        left_outer.grid(row=0, column=0, sticky="nsw", padx=(0, 12))
        left_outer.grid_propagate(False)
        left_outer.grid_rowconfigure(0, weight=1)
        left_outer.grid_columnconfigure(0, weight=1)

        self.left_scroll = ctk.CTkScrollableFrame(left_outer, corner_radius=18, fg_color="transparent")
        self.left_scroll.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        right = ctk.CTkFrame(content, corner_radius=20, fg_color=("#26282d", "#26282d"))
        right.grid(row=0, column=1, sticky="nsew")
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(2, weight=1)

        self.cfg_title = ctk.CTkLabel(self.left_scroll, text="", font=ctk.CTkFont(size=21, weight="bold"))
        self.cfg_title.pack(anchor="w", padx=10, pady=(12, 10))

        self.save_folder_label = ctk.CTkLabel(self.left_scroll, text="")
        self.save_folder_label.pack(anchor="w", padx=10)
        row = ctk.CTkFrame(self.left_scroll, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=(4, 10))
        self.source_entry = ctk.CTkEntry(row, textvariable=self.source_var, width=280, height=34)
        self.source_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))
        self.source_browse_btn = ctk.CTkButton(row, text="...", width=44, height=34, command=self.pick_source)
        self.source_browse_btn.pack(side="left")
        self.auto_detect_btn = ctk.CTkButton(self.left_scroll, text="", height=34, command=self.auto_detect_save_path)
        self.auto_detect_btn.pack(fill="x", padx=10, pady=(0, 10))

        self.backup_folder_label = ctk.CTkLabel(self.left_scroll, text="")
        self.backup_folder_label.pack(anchor="w", padx=10)
        row = ctk.CTkFrame(self.left_scroll, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=(4, 10))
        self.backup_entry = ctk.CTkEntry(row, textvariable=self.backup_var, width=280, height=34)
        self.backup_entry.pack(side="left", fill="x", expand=True, padx=(0, 8))
        self.backup_browse_btn = ctk.CTkButton(row, text="...", width=44, height=34, command=self.pick_backup)
        self.backup_browse_btn.pack(side="left")

        fields = ctk.CTkFrame(self.left_scroll, fg_color="transparent")
        fields.pack(fill="x", padx=10, pady=(0, 8))
        fields.grid_columnconfigure((0, 1), weight=1)

        self.interval_label = ctk.CTkLabel(fields, text="")
        self.interval_label.grid(row=0, column=0, sticky="w")
        self.max_label = ctk.CTkLabel(fields, text="")
        self.max_label.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.interval_entry = ctk.CTkEntry(fields, textvariable=self.interval_var, height=34)
        self.interval_entry.grid(row=1, column=0, sticky="ew", padx=(0, 8), pady=(4, 0))
        self.max_entry = ctk.CTkEntry(fields, textvariable=self.max_var, height=34)
        self.max_entry.grid(row=1, column=1, sticky="ew", pady=(4, 0))

        self.backup_on_start_check = ctk.CTkCheckBox(self.left_scroll, text="", variable=self.backup_on_start_var)
        self.backup_on_start_check.pack(anchor="w", padx=10, pady=(12, 4))
        self.only_if_changed_check = ctk.CTkCheckBox(self.left_scroll, text="", variable=self.only_if_changed_var)
        self.only_if_changed_check.pack(anchor="w", padx=10, pady=(0, 12))

        # language selector
        self.language_label = ctk.CTkLabel(self.left_scroll, text="")
        self.language_label.pack(anchor="w", padx=10, pady=(4, 6))
        lang_row = ctk.CTkFrame(self.left_scroll, fg_color="transparent")
        lang_row.pack(fill="x", padx=10, pady=(0, 14))
        self.language_segmented = ctk.CTkSegmentedButton(
            lang_row,
            values=list(LANGUAGE_OPTIONS.values()),
            variable=self.language_name_var,
            command=self.on_language_change
        )
        self.language_segmented.pack(fill="x", expand=True)

        self.save_settings_btn = ctk.CTkButton(self.left_scroll, text="", height=42, command=self.save_ui_config)
        self.save_settings_btn.pack(fill="x", padx=10, pady=(4, 18))

        self.actions_title = ctk.CTkLabel(self.left_scroll, text="", font=ctk.CTkFont(size=21, weight="bold"))
        self.actions_title.pack(anchor="w", padx=10, pady=(2, 10))
        self.start_btn = ctk.CTkButton(self.left_scroll, text="", height=44, command=self.start_monitor)
        self.start_btn.pack(fill="x", padx=10, pady=4)
        self.stop_btn = ctk.CTkButton(self.left_scroll, text="", height=44, fg_color="#991b1b", hover_color="#7f1d1d", command=self.stop_monitor)
        self.stop_btn.pack(fill="x", padx=10, pady=4)
        self.backup_now_btn = ctk.CTkButton(self.left_scroll, text="", height=44, command=self.manual_backup)
        self.backup_now_btn.pack(fill="x", padx=10, pady=4)
        self.restore_selected_btn = ctk.CTkButton(self.left_scroll, text="", height=44, command=self.restore_selected)
        self.restore_selected_btn.pack(fill="x", padx=10, pady=4)
        self.restore_healthy_btn = ctk.CTkButton(self.left_scroll, text="", height=44, fg_color="#2563eb", hover_color="#1d4ed8", command=self.restore_latest_healthy)
        self.restore_healthy_btn.pack(fill="x", padx=10, pady=4)
        self.validate_selected_btn = ctk.CTkButton(self.left_scroll, text="", height=40, fg_color="#3a3a3a", hover_color="#4a4a4a", command=self.verify_selected)
        self.validate_selected_btn.pack(fill="x", padx=10, pady=(16, 4))
        self.delete_selected_btn = ctk.CTkButton(self.left_scroll, text="", height=40, fg_color="#444444", hover_color="#525252", command=self.delete_selected_backup)
        self.delete_selected_btn.pack(fill="x", padx=10, pady=4)
        self.open_save_btn = ctk.CTkButton(self.left_scroll, text="", height=38, fg_color="#33363d", hover_color="#41454d", command=self.open_source_folder)
        self.open_save_btn.pack(fill="x", padx=10, pady=4)
        self.open_backup_btn = ctk.CTkButton(self.left_scroll, text="", height=38, fg_color="#33363d", hover_color="#41454d", command=self.open_backup_folder)
        self.open_backup_btn.pack(fill="x", padx=10, pady=4)
        self.export_log_btn = ctk.CTkButton(self.left_scroll, text="", height=38, fg_color="#33363d", hover_color="#41454d", command=self.export_log)
        self.export_log_btn.pack(fill="x", padx=10, pady=(18, 4))
        self.clear_log_btn = ctk.CTkButton(self.left_scroll, text="", height=38, fg_color="#33363d", hover_color="#41454d", command=self.clear_log)
        self.clear_log_btn.pack(fill="x", padx=10, pady=4)

        status_card = ctk.CTkFrame(right, corner_radius=20, fg_color=("#2b2d31", "#2b2d31"))
        status_card.grid(row=0, column=0, padx=14, pady=14, sticky="ew")
        for i in range(6):
            status_card.grid_columnconfigure(i, weight=1)

        self.status_title_label = None
        self.status_value_label = self._stat_box(status_card, 0, "status", self.status_var)
        self._stat_box(status_card, 1, "next_backup", self.countdown_var)
        self._stat_box(status_card, 2, "backups", self.backup_count_var)
        self._stat_box(status_card, 3, "save_size", self.save_size_var)
        self._stat_box(status_card, 4, "latest_healthy", self.healthy_backup_var)
        self._stat_box(status_card, 5, "latest_error", self.error_var)

        mid = ctk.CTkFrame(right, corner_radius=20, fg_color=("#2b2d31", "#2b2d31"))
        mid.grid(row=1, column=0, padx=14, pady=(0, 14), sticky="ew")
        mid.grid_columnconfigure(0, weight=1)
        mid.grid_columnconfigure(1, weight=1)
        mid.grid_columnconfigure(2, weight=0)
        mid.grid_columnconfigure(3, weight=0)

        self.latest_backup_label = ctk.CTkLabel(mid, text="", font=ctk.CTkFont(size=13, weight="bold"))
        self.latest_backup_label.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        ctk.CTkLabel(mid, textvariable=self.last_backup_var, font=ctk.CTkFont(size=13)).grid(row=1, column=0, sticky="w", padx=14, pady=(0, 12))

        self.search_label = ctk.CTkLabel(mid, text="", font=ctk.CTkFont(size=13, weight="bold"))
        self.search_label.grid(row=0, column=1, sticky="w", padx=14, pady=(12, 2))
        self.search_entry = ctk.CTkEntry(mid, textvariable=self.search_var, height=34)
        self.search_entry.grid(row=1, column=1, sticky="ew", padx=14, pady=(0, 12))
        self.search_entry.bind("<KeyRelease>", lambda e: self.refresh_all())

        self.only_healthy_check = ctk.CTkCheckBox(mid, text="", variable=self.only_healthy_var, command=self.refresh_all)
        self.only_healthy_check.grid(row=1, column=2, sticky="w", padx=(6, 8), pady=(0, 12))

        self.refresh_btn = ctk.CTkButton(mid, text="", width=130, command=self.refresh_all)
        self.refresh_btn.grid(row=0, column=3, rowspan=2, sticky="e", padx=14)

        bottom = ctk.CTkFrame(right, corner_radius=20, fg_color=("#2b2d31", "#2b2d31"))
        bottom.grid(row=2, column=0, padx=14, pady=(0, 14), sticky="nsew")
        bottom.grid_columnconfigure(0, weight=5)
        bottom.grid_columnconfigure(1, weight=1)
        bottom.grid_rowconfigure(1, weight=1)

        self.available_backups_label = ctk.CTkLabel(bottom, text="", font=ctk.CTkFont(size=18, weight="bold"))
        self.available_backups_label.grid(row=0, column=0, sticky="w", padx=14, pady=12)
        self.log_label = ctk.CTkLabel(bottom, text="", font=ctk.CTkFont(size=18, weight="bold"))
        self.log_label.grid(row=0, column=1, sticky="w", padx=14, pady=12)

        tree_frame = ctk.CTkFrame(bottom, corner_radius=12, fg_color=("#111827", "#111827"))
        tree_frame.grid(row=1, column=0, sticky="nsew", padx=(14, 7), pady=(0, 14))
        tree_frame.grid_columnconfigure(0, weight=1)
        tree_frame.grid_rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            tree_frame,
            columns=("name", "date", "size", "healthy", "path"),
            show="headings",
            style="Guardian.Treeview"
        )
        self.tree.heading("name", text="", command=lambda: self.sort_by("name"))
        self.tree.heading("date", text="", command=lambda: self.sort_by("date"))
        self.tree.heading("size", text="", command=lambda: self.sort_by("size"))
        self.tree.heading("healthy", text="", command=lambda: self.sort_by("healthy"))
        self.tree.heading("path", text="", command=lambda: self.sort_by("path"))
        self.tree.column("name", width=220, anchor="w")
        self.tree.column("date", width=170, anchor="w")
        self.tree.column("size", width=95, anchor="w")
        self.tree.column("healthy", width=85, anchor="center")
        self.tree.column("path", width=620, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=0, column=1, sticky="ns")

        self.tree.tag_configure("healthy", background="#0e2e1f", foreground="#bbf7d0")
        self.tree.tag_configure("unhealthy", background="#3b1111", foreground="#fecaca")
        self.tree.bind("<Double-1>", self.on_tree_double_click)

        log_frame = ctk.CTkFrame(bottom, corner_radius=12, fg_color=("#17191d", "#17191d"))
        log_frame.grid(row=1, column=1, sticky="nsew", padx=(7, 14), pady=(0, 14))
        log_frame.grid_columnconfigure(0, weight=1)
        log_frame.grid_rowconfigure(0, weight=1)

        self.log_box = ctk.CTkTextbox(log_frame, font=ctk.CTkFont(size=11), corner_radius=10)
        self.log_box.grid(row=0, column=0, sticky="nsew")
        self.log_box.configure(state="disabled")

        footer = ctk.CTkFrame(self, corner_radius=16, fg_color=("#1f2937", "#1f2937"), height=38)
        footer.grid(row=2, column=0, padx=16, pady=(0, 16), sticky="ew")
        footer.grid_propagate(False)
        footer.grid_columnconfigure(0, weight=1)

        self.footer_label = ctk.CTkLabel(footer, textvariable=self.footer_var, anchor="w", font=ctk.CTkFont(size=12))
        self.footer_label.grid(row=0, column=0, sticky="ew", padx=14, pady=8)

    def _stat_box(self, parent, col, title_key, var):
        box = ctk.CTkFrame(parent, corner_radius=14, fg_color=("#24262b", "#24262b"))
        box.grid(row=0, column=col, padx=8, pady=10, sticky="ew")
        title = ctk.CTkLabel(box, text=title_key, font=ctk.CTkFont(size=12))
        title.pack(anchor="w", padx=10, pady=(8, 0))
        value_label = ctk.CTkLabel(box, textvariable=var, font=ctk.CTkFont(size=15, weight="bold"), wraplength=185)
        value_label.pack(anchor="w", padx=10, pady=(4, 10))
        if title_key == "status":
            self.status_title_label = title
        setattr(self, f"stat_title_{col}", title)
        return value_label

    def apply_language(self):
        self.title(self.tr("app_title"))
        self.header_title.configure(text=self.tr("app_title"))
        self.cfg_title.configure(text=self.tr("configurations"))
        self.save_folder_label.configure(text=self.tr("save_folder"))
        self.backup_folder_label.configure(text=self.tr("backup_folder"))
        self.interval_label.configure(text=self.tr("interval_min"))
        self.max_label.configure(text=self.tr("max_backups"))
        self.backup_on_start_check.configure(text=self.tr("backup_on_start"))
        self.only_if_changed_check.configure(text=self.tr("only_if_changed"))
        self.language_label.configure(text=self.tr("language"))
        self.save_settings_btn.configure(text=self.tr("save_settings"))
        self.actions_title.configure(text=self.tr("actions"))
        self.start_btn.configure(text=self.tr("start_auto_backup"))
        self.stop_btn.configure(text=self.tr("stop_auto_backup"))
        self.backup_now_btn.configure(text=self.tr("backup_now"))
        self.restore_selected_btn.configure(text=self.tr("restore_selected"))
        self.restore_healthy_btn.configure(text=self.tr("restore_latest_healthy"))
        self.validate_selected_btn.configure(text=self.tr("validate_selected"))
        self.delete_selected_btn.configure(text=self.tr("delete_selected"))
        self.open_save_btn.configure(text=self.tr("open_save_folder"))
        self.open_backup_btn.configure(text=self.tr("open_backup_folder"))
        self.auto_detect_btn.configure(text=self.tr("auto_detect"))
        self.export_log_btn.configure(text=self.tr("export_log"))
        self.clear_log_btn.configure(text=self.tr("clear_log"))
        self.status_title_label.configure(text=self.tr("status"))
        self.stat_title_1.configure(text=self.tr("next_backup"))
        self.stat_title_2.configure(text=self.tr("backups"))
        self.stat_title_3.configure(text=self.tr("save_size"))
        self.stat_title_4.configure(text=self.tr("latest_healthy"))
        self.stat_title_5.configure(text=self.tr("latest_error"))
        self.latest_backup_label.configure(text=self.tr("latest_backup"))
        self.search_label.configure(text=self.tr("search_backup"))
        self.search_entry.configure(placeholder_text=self.tr("search_placeholder"))
        self.only_healthy_check.configure(text=self.tr("only_healthy"))
        self.refresh_btn.configure(text=self.tr("refresh_list"))
        self.available_backups_label.configure(text=self.tr("available_backups"))
        self.log_label.configure(text=self.tr("log"))
        self.tree.heading("name", text=self.tr("build_name"), command=lambda: self.sort_by("name"))
        self.tree.heading("date", text=self.tr("build_date"), command=lambda: self.sort_by("date"))
        self.tree.heading("size", text=self.tr("build_size"), command=lambda: self.sort_by("size"))
        self.tree.heading("healthy", text=self.tr("build_healthy"), command=lambda: self.sort_by("healthy"))
        self.tree.heading("path", text=self.tr("build_path"), command=lambda: self.sort_by("path"))
        self.status_var.set(self.tr("active") if self.service.running else self.tr("stopped"))
        if self.error_var.get() in ("none", "nenhum"):
            self.error_var.set(self.tr("none"))
        self.footer_var.set(self.tr("ready"))

    def on_language_change(self, selected_name: str):
        for code, name in LANGUAGE_OPTIONS.items():
            if name == selected_name:
                self.lang = code
                self.service.config.language = code
                self.service.save_config()
                self.apply_language()
                self.log(self.tr("language_changed", language=name))
                self.refresh_all()
                break

    def log(self, text: str):
        self.after(0, lambda: self._append_log(text))

    def _append_log(self, text: str):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", text + "\n")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")
        self.footer_var.set(text)

    def set_footer(self, text: str):
        self.footer_var.set(text)

    def export_log(self):
        from tkinter import filedialog
        path = filedialog.asksaveasfilename(
            title=self.tr("save_log_dialog_title"),
            initialfile=self.tr("log_file_name"),
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if not path:
            return
        content = self.log_box.get("1.0", "end").strip()
        Path(path).write_text(content, encoding="utf-8")
        self.show_custom_dialog(self.tr("success"), self.tr("log_exported"), kind="success")

    def clear_log(self):
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")
        self.set_footer(self.tr("log_cleared"))

    def update_countdown(self, seconds: Optional[int]):
        def _do():
            if seconds is None:
                self.countdown_var.set("-")
            else:
                m, s = divmod(seconds, 60)
                h, m = divmod(m, 60)
                self.countdown_var.set(f"{h:02d}:{m:02d}:{s:02d}")
        self.after(0, _do)

    def auto_detect_save_path(self, silent: bool = False):
        detected = detect_default_save_path()
        if detected:
            self.source_var.set(str(detected))
            if not silent:
                self.show_custom_dialog(self.tr("success"), self.tr("save_detected_using", path=str(detected)), kind="success")
            self.set_footer(self.tr("save_detected"))
            return True
        else:
            if not silent:
                self.show_custom_dialog(self.tr("warning"), self.tr("save_not_detected"), kind="warning")
            self.set_footer(self.tr("save_not_detected"))
            return False

    def pick_source(self):
        path = filedialog.askdirectory(title=self.tr("select_save_folder"))
        if path:
            self.source_var.set(path)

    def pick_backup(self):
        path = filedialog.askdirectory(title=self.tr("select_backup_folder"))
        if path:
            self.backup_var.set(path)

    def load_config_to_ui(self):
        cfg = self.service.config
        self.source_var.set(cfg.source_dir)
        self.backup_var.set(cfg.backup_dir)
        self.interval_var.set(str(cfg.interval_minutes))
        self.max_var.set(str(cfg.max_versions))
        self.backup_on_start_var.set(cfg.backup_on_start)
        self.only_if_changed_var.set(cfg.only_if_changed)
        self.lang = cfg.language if cfg.language in LANGUAGE_OPTIONS else "pt-BR"
        self.language_name_var.set(LANGUAGE_OPTIONS[self.lang])

    def set_config_controls_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for widget in [
            self.source_entry,
            self.backup_entry,
            self.interval_entry,
            self.max_entry,
            self.backup_on_start_check,
            self.only_if_changed_check,
            self.language_segmented,
        ]:
            widget.configure(state=state)

    def update_status_visual(self):
        if self.service.running:
            self.status_value_label.configure(text_color="#86efac")
        else:
            self.status_value_label.configure(text_color="#fca5a5")

    def save_ui_config(self):
        cfg = Config(
            source_dir=self.source_var.get().strip(),
            backup_dir=self.backup_var.get().strip(),
            interval_minutes=max(1, int(self.interval_var.get().strip())),
            max_versions=max(1, int(self.max_var.get().strip())),
            backup_on_start=bool(self.backup_on_start_var.get()),
            only_if_changed=bool(self.only_if_changed_var.get()),
            language=self.lang,
        )
        was_running = self.service.running
        if was_running:
            self.service.stop()
        self.service.config = cfg
        self.service.save_config()
        if was_running:
            self.service.start()
        self.log(self.tr("config_saved_log"))
        self.refresh_all()

    def start_monitor(self):
        try:
            self.save_ui_config()
            self.service.start()
            self.set_config_controls_state(False)
            self.refresh_all()
        except Exception as exc:
            self.show_custom_dialog(self.tr("error"), str(exc), kind="error")

    def stop_monitor(self):
        self.service.stop()
        self.set_config_controls_state(True)
        self.refresh_all()

    def manual_backup(self):
        try:
            self.save_ui_config()
            result = self.service.create_backup(manual=True)
            if result:
                self.show_custom_dialog(self.tr("success"), self.tr("backup_created"), kind="success")
            else:
                self.show_custom_dialog(self.tr("info"), self.tr("no_changes"), kind="info")
        except Exception as exc:
            self.show_custom_dialog(self.tr("error"), f"{exc}", kind="error")

    def _selected_backup_path(self) -> Optional[Path]:
        selected = self.tree.selection()
        if not selected:
            return None
        values = self.tree.item(selected[0], "values")
        return Path(values[4])

    def verify_selected(self):
        backup_path = self._selected_backup_path()
        if not backup_path:
            self.show_custom_dialog(self.tr("warning"), self.tr("select_backup"), kind="warning")
            return
        ok, reason = self.service.verify_backup_integrity(backup_path)
        if ok:
            self.show_custom_dialog(self.tr("integrity"), self.tr("backup_integrity_ok", backup=backup_path.name), kind="success")
        else:
            self.show_custom_dialog(self.tr("integrity"), self.tr("backup_integrity_bad", reason=reason), kind="error")

    def restore_selected(self):
        backup_path = self._selected_backup_path()
        if not backup_path:
            self.show_custom_dialog(self.tr("warning"), self.tr("select_backup"), kind="warning")
            return

        confirm = self.ask_custom_confirm(self.tr("confirm_restore_title"), self.tr("confirm_restore_msg"))
        if not confirm:
            return

        try:
            self.service.restore_backup(backup_path)
            self.show_custom_dialog(self.tr("success"), self.tr("restore_success"), kind="success")
        except Exception as exc:
            self.show_custom_dialog(self.tr("error"), f"{exc}", kind="error")

    def restore_latest_healthy(self):
        confirm = self.ask_custom_confirm(self.tr("confirm_restore_healthy_title"), self.tr("confirm_restore_healthy_msg"))
        if not confirm:
            return
        try:
            restored = self.service.restore_latest_healthy_backup()
            self.show_custom_dialog(self.tr("success"), self.tr("restore_success_using", backup=restored.name), kind="success")
        except Exception as exc:
            self.show_custom_dialog(self.tr("error"), str(exc), kind="error")

    def delete_selected_backup(self):
        backup_path = self._selected_backup_path()
        if not backup_path:
            self.show_custom_dialog(self.tr("warning"), self.tr("select_backup"), kind="warning")
            return

        confirm = self.ask_custom_confirm(self.tr("confirm_delete_title"), self.tr("confirm_delete_msg", backup=backup_path.name))
        if not confirm:
            return

        try:
            shutil.rmtree(backup_path)
            self.log(self.tr("delete_success_log", backup=backup_path.name))
            self.refresh_all()
        except Exception as exc:
            self.show_custom_dialog(self.tr("error"), f"{exc}", kind="error")

    def on_tree_double_click(self, event=None):
        backup_path = self._selected_backup_path()
        if not backup_path:
            return

        dialog = ctk.CTkToplevel(self)
        dialog.title(self.tr("quick_action"))
        dialog.geometry("420x220")
        dialog.transient(self)
        dialog.grab_set()

        frame = ctk.CTkFrame(dialog, corner_radius=16)
        frame.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(frame, text=self.tr("quick_action"), font=ctk.CTkFont(size=20, weight="bold")).pack(anchor="w", padx=16, pady=(16, 8))
        ctk.CTkLabel(frame, text=self.tr("quick_action_backup", backup=backup_path.name), justify="left").pack(anchor="w", padx=16, pady=(0, 18))

        button_row = ctk.CTkFrame(frame, fg_color="transparent")
        button_row.pack(fill="x", padx=16, pady=(0, 16))
        button_row.grid_columnconfigure((0, 1, 2), weight=1)

        ctk.CTkButton(button_row, text=self.tr("restore"), command=lambda: [dialog.destroy(), self.restore_selected()]).grid(row=0, column=0, padx=6, sticky="ew")
        ctk.CTkButton(button_row, text=self.tr("validate"), command=lambda: [dialog.destroy(), self.verify_selected()]).grid(row=0, column=1, padx=6, sticky="ew")
        ctk.CTkButton(button_row, text=self.tr("close"), fg_color="#444444", hover_color="#525252", command=dialog.destroy).grid(row=0, column=2, padx=6, sticky="ew")

    def show_custom_dialog(self, title: str, message: str, kind: str = "info"):
        dialog = ctk.CTkToplevel(self)
        dialog.title(title)
        dialog.geometry("460x240")
        dialog.transient(self)
        dialog.grab_set()

        color_map = {"info": "#2563eb", "success": "#16a34a", "warning": "#d97706", "error": "#dc2626"}
        accent = color_map.get(kind, "#2563eb")

        frame = ctk.CTkFrame(dialog, corner_radius=16)
        frame.pack(fill="both", expand=True, padx=16, pady=16)

        top = ctk.CTkFrame(frame, corner_radius=12, fg_color=accent, height=10)
        top.pack(fill="x", padx=16, pady=(16, 12))

        ctk.CTkLabel(frame, text=title, font=ctk.CTkFont(size=20, weight="bold")).pack(anchor="w", padx=16, pady=(4, 8))
        ctk.CTkLabel(frame, text=message, justify="left", wraplength=400).pack(anchor="w", padx=16, pady=(0, 18))

        ctk.CTkButton(frame, text=self.tr("ok"), width=110, command=dialog.destroy).pack(padx=16, pady=(0, 16), anchor="e")

    def ask_custom_confirm(self, title: str, message: str) -> bool:
        result = {"value": False}

        dialog = ctk.CTkToplevel(self)
        dialog.title(title)
        dialog.geometry("500x260")
        dialog.transient(self)
        dialog.grab_set()

        frame = ctk.CTkFrame(dialog, corner_radius=16)
        frame.pack(fill="both", expand=True, padx=16, pady=16)

        top = ctk.CTkFrame(frame, corner_radius=12, fg_color="#d97706", height=10)
        top.pack(fill="x", padx=16, pady=(16, 12))

        ctk.CTkLabel(frame, text=title, font=ctk.CTkFont(size=20, weight="bold")).pack(anchor="w", padx=16, pady=(4, 8))
        ctk.CTkLabel(frame, text=message, justify="left", wraplength=430).pack(anchor="w", padx=16, pady=(0, 20))

        buttons = ctk.CTkFrame(frame, fg_color="transparent")
        buttons.pack(fill="x", padx=16, pady=(0, 16))
        buttons.grid_columnconfigure((0, 1), weight=1)

        def yes():
            result["value"] = True
            dialog.destroy()

        def no():
            result["value"] = False
            dialog.destroy()

        ctk.CTkButton(buttons, text=self.tr("confirm"), command=yes).grid(row=0, column=0, padx=(0, 6), sticky="ew")
        ctk.CTkButton(buttons, text=self.tr("cancel"), fg_color="#444444", hover_color="#525252", command=no).grid(row=0, column=1, padx=(6, 0), sticky="ew")

        self.wait_window(dialog)
        return result["value"]

    def open_source_folder(self):
        self._open_folder(self.source_var.get().strip())

    def open_backup_folder(self):
        self._open_folder(self.backup_var.get().strip())

    def _open_folder(self, path: str):
        if not path:
            return
        p = Path(path)
        if not p.exists():
            self.show_custom_dialog(self.tr("warning"), self.tr("folder_not_found", path=path), kind="warning")
            return
        try:
            os.startfile(str(p))
        except Exception:
            subprocess.Popen(["explorer", str(p)])

    def _backup_matches_filter(self, values, healthy_ok: bool):
        query = self.search_var.get().strip().lower()
        if self.only_healthy_var.get() and not healthy_ok:
            return False
        if not query:
            return True
        haystack = " ".join(str(v).lower() for v in values)
        return query in haystack

    def _sort_key(self, item):
        if self.sort_column == "name":
            return item["name"].lower()
        if self.sort_column == "date":
            return item["timestamp"]
        if self.sort_column == "size":
            return item["size_bytes"]
        if self.sort_column == "healthy":
            return 1 if item["healthy_ok"] else 0
        if self.sort_column == "path":
            return item["path"].lower()
        return item["timestamp"]

    def sort_by(self, column: str):
        if self.sort_column == column:
            self.sort_desc = not self.sort_desc
        else:
            self.sort_column = column
            self.sort_desc = True
        self.refresh_all()

    def refresh_all(self):
        self.after(0, self._refresh_all)

    def _refresh_all(self):
        source = Path(self.source_var.get().strip()) if self.source_var.get().strip() else None
        backups = self.service.list_backups()
        healthy = self.service.latest_healthy_backup()

        self.status_var.set(self.tr("active") if self.service.running else self.tr("stopped"))
        self.last_backup_var.set(self.service.last_backup_time or "-")
        self.error_var.set((self.service.last_error or self.tr("none"))[:40])
        self.backup_count_var.set(str(len(backups)))
        self.healthy_backup_var.set(healthy.name if healthy else "-")

        if source and source.exists():
            self.save_size_var.set(human_size(folder_size(source)))
        else:
            self.save_size_var.set(self.tr("missing_folder"))

        for item in self.tree.get_children():
            self.tree.delete(item)

        items = []
        visible_count = 0

        for bkp in backups:
            try:
                created_dt = datetime.fromtimestamp(bkp.stat().st_ctime)
                created = created_dt.strftime("%d/%m/%Y %H:%M:%S") + f" • {human_relative_time(created_dt, self.lang)}"
                timestamp = created_dt.timestamp()
            except Exception:
                created = "-"
                timestamp = 0

            size_bytes = folder_size(bkp)
            size_text = human_size(size_bytes)
            ok, _ = self.service.verify_backup_integrity(bkp)
            healthy_text = self.tr("yes") if ok else self.tr("no")

            values = (bkp.name, created, size_text, healthy_text, str(bkp))
            if self._backup_matches_filter(values, ok):
                items.append({
                    "name": bkp.name,
                    "date": created,
                    "timestamp": timestamp,
                    "size_text": size_text,
                    "size_bytes": size_bytes,
                    "healthy_text": healthy_text,
                    "healthy_ok": ok,
                    "path": str(bkp),
                })

        items.sort(key=self._sort_key, reverse=self.sort_desc)

        for item in items:
            tag = "healthy" if item["healthy_ok"] else "unhealthy"
            self.tree.insert(
                "",
                "end",
                values=(item["name"], item["date"], item["size_text"], item["healthy_text"], item["path"]),
                tags=(tag,)
            )
            visible_count += 1

        self.set_config_controls_state(not self.service.running)
        self.start_btn.configure(state="disabled" if self.service.running else "normal")
        self.stop_btn.configure(state="normal" if self.service.running else "disabled")
        self.update_status_visual()
        self.set_footer(
            self.tr(
                "showing_backups",
                count=visible_count,
                column=self.sort_column,
                order=self.tr("sort_desc") if self.sort_desc else self.tr("sort_asc"),
            )
        )


if __name__ == "__main__":
    app = App()
    app.mainloop()

import json
import os
import logging
import hashlib
import click
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union, Optional, Any, Generator

class CyyLog:
    """
    A data generator and processor class that reads data from JSON file,
    processes items, and stores processed results.
    """
    
    def __init__(self, input_path: str = None, output_path: str = 'output.json'):
        """
        Initialize CyLog with input and output JSON file paths.
        
        Args:
            input_path: Path to input JSON file containing list of dictionaries
            output_path: Path to output JSON file for storing processed data
        """
        self.input_path = input_path
        self.output_path = output_path
        self._setup_logger()
        self._ensure_output_directory()
        self.output_data = self._load_output_data()
        
    def _setup_logger(self) -> None:
        """Configure colorful and informative logger."""
        self.logger = logging.getLogger('CyLog')
        self.logger.setLevel(logging.INFO)
        
        if self.logger.handlers:
            self.logger.handlers.clear()
            
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                     datefmt='%m/%d/%Y %I:%M:%S %p')
        
        console.setFormatter(formatter)
        self.logger.addHandler(console)
        
        self.logger.propagate = False
    
    def _ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        output_dir = os.path.dirname(self.output_path)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    def _load_output_data(self) -> Dict[str, List[Dict]]:
        """Load existing output data or initialize empty structure."""
        try:
            with open(self.output_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"dones": []}
    
    def _save_output_data(self) -> None:
        """Save current output data to file."""
        with open(self.output_path, 'w', encoding='utf-8') as f:
            json.dump(self.output_data, f, indent=2, ensure_ascii=False)
    
    def _get_item_id(self, item: Any) -> str:
        """
        Generate MD5 hash ID for an item.
        
        Args:
            item: Any item to be hashed
            
        Returns:
            MD5 hash string of the item
        """
        item_str = json.dumps(item, sort_keys=True, default=str)
        return hashlib.md5(item_str.encode('utf-8')).hexdigest()
    
    def _format_output_entry(self, item: Any, message: Optional[str] = None) -> Dict:
        """
        Format an item for output storage.
        
        Args:
            item: The item to be stored
            message: Optional message to include
            
        Returns:
            Dictionary with timestamp, ID, message and item
        """
        output = {
            "timestamp": datetime.now().isoformat(),
            "id": self._get_item_id(item),
            "item": item
        }
        
        if message:
            output["message"] = message
            
        return output
    
    def endpoint(self, item: Any, keys: Union[str, List[str]] = "endpoint", message: Optional[str] = None) -> None:
        """
        Process an item and store it under specified key(s).
        
        Args:
            item: The item to be processed
            keys: Single key or list of keys to store the item under
            message: Optional message to include with the item
        """
        if not item:
            self.logger.warning(click.style("Empty item provided to endpoint, skipping", fg="yellow"))
            return
            
        if isinstance(keys, str):
            keys = [keys]
            
        entry = self._format_output_entry(item, message)
        
        for key in keys:
            if key not in self.output_data:
                self.output_data[key] = []
            
            self.output_data[key].append(entry)
            
        self._save_output_data()
        
        total = sum(len(self.output_data.get(k, [])) for k in keys)
        self.logger.info(
            f'[ {click.style("Endpoint", fg="bright_green")} ] :: '
            f'{click.style("Keys", fg="magenta")}: [ {", ".join(keys)} ] | '
            f'{click.style("Total", fg="bright_blue")}: [ {total} ]'
        )
    
    def check(self, item: Any, keys: Union[str, List[str]] = 'endpoint') -> bool:
        """
        Check if an item exists in specified output key(s).
        
        Args:
            item: The item to check
            keys: Single key or list of keys to check against
            
        Returns:
            True if item exists in any of the specified keys, False otherwise
        """
        if isinstance(keys, str):
            keys = [keys]
            
        item_id = self._get_item_id(item)
        
        for key in keys:
            if key not in self.output_data:
                self.logger.warning(
                    f'[ {click.style("Check", fg="yellow")} ] :: '
                    f'Key "{key}" does not exist in output data'
                )
                continue
                
            if any(entry.get("id") == item_id for entry in self.output_data[key]):
                self.logger.info(
                    f'[ {click.style("Check", fg="bright_green")} ] :: '
                    f'Item found with ID [ {item_id} ] in key [ {click.style(key, fg="cyan")} ]'
                )
                return True
                
        return False
    
    def done(self, item: Any, message: Optional[str] = None) -> None:
        """
        Mark an item as done and save it.
        
        Args:
            item: The item to mark as done
            message: Optional message to include with the item
        """
        if not item:
            self.logger.warning(click.style("Empty item provided to done, skipping", fg="yellow"))
            return
            
        entry = self._format_output_entry(item, message)
        self.output_data["dones"].append(entry)
        self._save_output_data()
        
        total = len(self.output_data["dones"])
        self.logger.info(
            f'[ {click.style("Done", fg="bright_green")} ] :: '
            f'{click.style("Total", fg="magenta")}: [ {total} ]'
        )
    
    def generator(self) -> Generator[Dict, None, None]:
        """
        Generate items from the input file, skipping those already processed.
        
        Yields:
            Items from input file that haven't been processed yet
        """
        try:
            with open(self.input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not isinstance(data, list):
                self.logger.error(click.style("Input file must contain a list of items", fg="red"))
                return
                
            total = len(data)
            processed = 0
            skipped = 0
            
            for index, item in enumerate(data):
                if not item:
                    self.logger.warning(
                        f'[ {click.style("Generator", fg="yellow")} ] :: '
                        f'Empty item at index {index}, skipping'
                    )
                    skipped += 1
                    continue
                    
                item_id = self._get_item_id(item)
                
                if any(entry.get("id") == item_id for entry in self.output_data["dones"]):
                    skipped += 1
                    self.logger.info(
                        f'[ {click.style("Check", fg="bright_green")} ] :: '
                        f'Item found with ID [ {item_id} ] in key [ {click.style("dones", fg="cyan")} ]'
                    )
                    continue
                    
                processed += 1
                
                item_preview = str(item)[:100] + "..." if len(str(item)) > 100 else str(item)
                self.logger.info(
                    f'[ {click.style("Item Preview", fg="bright_magenta")} ]: {item_preview}'
                )
                
                self.logger.info(
                    f'[ {click.style("Generator", fg="bright_green")} ] :: '
                    f'{click.style("Processing", fg="cyan")}: Item index [ {index} ] | '
                    f'{click.style("ITEM", fg="bright_blue")}: [ {item_id} ]'
                )
                yield item
                
            self.logger.info(
                f'[ {click.style("Generator", fg="bright_green")} ] :: '
                f'{click.style("Summary", fg="magenta")}: '
                f'Total: [ {total} ] | '
                f'{click.style("Processed", fg="bright_blue")}: [ {processed} ] | '
                f'{click.style("Skipped", fg="yellow")}: [ {skipped} ]'
            )
            
        except FileNotFoundError:
            self.logger.error(click.style(f"Input file not found: {self.input_path}", fg="red"))
            self.logger.error(click.style(f"if you use the generator you have to enter the input file!", fg="red"))
        except json.JSONDecodeError:
            self.logger.error(click.style(f"Invalid JSON in input file: {self.input_path}", fg="red"))
            
if __name__ == '__main__':
    generator = CyyLog("queque.json", "output.json")
    for item in generator.generator():
        try:
            generator.done(item)
        except Exception as err:
            generator.done(item, str(err))
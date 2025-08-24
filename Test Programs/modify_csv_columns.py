import pandas as pd
import re
import os

def extract_brand_model_color(text):
    """
    Extract brand, model, and color from smartphone description text.
    Returns tuple: (brand, model, color)
    """
    if not text or pd.isna(text):
        return None, None, None
    
    text = str(text).strip()
    
    # Common smartphone brands
    brands = [
        'Apple', 'Samsung', 'Redmi', 'Xiaomi', 'OnePlus', 'Oppo', 'Vivo', 
        'Realme', 'Huawei', 'Honor', 'Nokia', 'Motorola', 'Google', 'Sony',
        'LG', 'HTC', 'Asus', 'Nothing', 'iQOO', 'Poco', 'Mi', 'Infinix',
        'Tecno', 'Lava', 'Micromax', 'Intex', 'Karbonn', 'Gionee', 'Lenovo',
        'ZTE', 'Alcatel', 'BlackBerry', 'Meizu', 'LeEco', 'Coolpad', 'YU',
        'InFocus', 'Panasonic', 'Celkon', 'Spice', 'Xolo', 'iBall', 'Swipe',
        'Lyf', 'Jio', 'Airtel', 'BSNL', 'Idea', 'Vodafone', 'Reliance',
        'Smartron', 'Ziox', 'Kult', 'Comio', 'Mobiistar', 'Nubia', 'Sharp',
        'Fairphone', 'Essential', 'Razer', 'ROG', 'Legion', 'RedMagic',
        'Black Shark', 'Gaming Phone', 'Rugged', 'CAT', 'Doogee', 'Ulefone',
        'Oukitel', 'Blackview', 'Cubot', 'Elephone', 'Vernee', 'Bluboo',
        'Homtom', 'Leagoo', 'Maze', 'Nomu', 'Poptel', 'Conquest', 'AGM',
        'Crosscall', 'Kyocera', 'Sonim', 'Caterpillar', 'Land Rover'
    ]
    
    # Common colors (case insensitive)
    colors = [
        'Black', 'White', 'Blue', 'Red', 'Green', 'Yellow', 'Pink', 'Purple',
        'Gold', 'Silver', 'Rose Gold', 'Space Gray', 'Midnight', 'Starlight',
        'Sky Blue', 'Sea Blue', 'Nature Green', 'Aqua Green', 'Coral',
        'Graphite', 'Sierra Blue', 'Alpine Green', 'Deep Purple', 'Orange',
        'Brown', 'Beige', 'Cream', 'Ivory', 'Pearl', 'Bronze', 'Copper',
        'Champagne', 'Platinum', 'Titanium', 'Steel', 'Matte Black', 'Glossy Black',
        'Jet Black', 'Carbon Black', 'Onyx', 'Obsidian', 'Coal', 'Charcoal',
        'Pure White', 'Snow White', 'Pearl White', 'Frost White', 'Ceramic White',
        'Ocean Blue', 'Navy Blue', 'Royal Blue', 'Electric Blue', 'Cobalt Blue',
        'Teal', 'Turquoise', 'Cyan', 'Azure', 'Sapphire', 'Indigo',
        'Crimson', 'Scarlet', 'Cherry Red', 'Wine Red', 'Burgundy', 'Maroon',
        'Forest Green', 'Mint Green', 'Lime Green', 'Emerald', 'Jade', 'Olive',
        'Hot Pink', 'Magenta', 'Fuchsia', 'Rose', 'Blush', 'Salmon',
        'Lavender', 'Violet', 'Plum', 'Orchid', 'Lilac', 'Amethyst',
        'Sunset Orange', 'Peach', 'Apricot', 'Tangerine', 'Amber', 'Honey',
        'Lemon', 'Canary', 'Mustard', 'Saffron', 'Marigold', 'Sunflower',
        'Gradient', 'Aurora', 'Prism', 'Rainbow', 'Holographic', 'Iridescent',
        'Matte', 'Glossy', 'Satin', 'Metallic', 'Shimmer', 'Sparkle',
        'Transparent', 'Clear', 'Frosted', 'Smoke', 'Haze', 'Mist'
    ]
    
    brand = None
    model = None
    color = None
    
    # Extract brand (usually at the beginning)
    for b in brands:
        if text.lower().startswith(b.lower()):
            brand = b
            break
    
    # Extract color (look for color keywords)
    text_lower = text.lower()
    for c in colors:
        if c.lower() in text_lower:
            color = c
            break
    
    # Extract model (everything after brand, before color or specifications)
    if brand:
        # Remove brand from beginning
        remaining = text[len(brand):].strip()
        
        # Remove common prefixes like "iPhone"
        if brand == 'Apple' and remaining.lower().startswith('iphone'):
            remaining = remaining[6:].strip()
            model_prefix = 'iPhone'
        else:
            model_prefix = ''
        
        # Remove color from the end if found
        if color:
            color_pattern = re.compile(re.escape(color), re.IGNORECASE)
            remaining = color_pattern.sub('', remaining).strip()
        
        # Remove specifications in parentheses and brackets
        remaining = re.sub(r'\([^)]*\)', '', remaining)
        remaining = re.sub(r'\[[^\]]*\]', '', remaining)
        
        # Remove common specification patterns
        remaining = re.sub(r'\d+GB.*', '', remaining)
        remaining = re.sub(r'\d+MP.*', '', remaining)
        remaining = re.sub(r'\d+mAh.*', '', remaining)
        remaining = re.sub(r'\|\s*.*', '', remaining)  # Remove everything after |
        remaining = re.sub(r'-.*', '', remaining)      # Remove everything after -
        
        # Clean up extra spaces and punctuation
        remaining = re.sub(r'[,\-\|].*', '', remaining)
        remaining = remaining.strip(' ,.-')
        
        if model_prefix:
            model_part = f"{model_prefix} {remaining}".strip()
        else:
            model_part = remaining.strip()
        
        # Concatenate brand + model for the model column
        if brand != "Apple":
            model = f"{brand} {model_part}".strip()
        
        else:
            model = f"{model_part}".strip()
    
    # If no brand found, try to extract from the beginning of the text
    if not brand:
        words = text.split()
        if words:
            potential_brand = words[0]
            if potential_brand.istitle() and len(potential_brand) > 2:
                brand = potential_brand
                # Try to get model from remaining words
                remaining_words = words[1:]
                if remaining_words:
                    model_part = ' '.join(remaining_words)
                    # Clean model similar to above
                    model_part = re.sub(r'\([^)]*\)', '', model_part)
                    model_part = re.sub(r'\[[^\]]*\]', '', model_part)
                    if color:
                        color_pattern = re.compile(re.escape(color), re.IGNORECASE)
                        model_part = color_pattern.sub('', model_part).strip()
                    model_part = model_part.strip(' ,.-')
                    
                    # Concatenate brand + model for the model column
                    if brand != "Apple":
                        model = f"{brand} {model_part}".strip()
                    
                    else:
                        model = f"{model_part}".strip()
    
    return brand, model, color

# def modify_amazon_csv():
#     """Modify Amazon Top Rated Smartphones CSV file"""
#     file_path = r"e:\BenchSmart\Test Programs\kaggle_datasets\smartphones.csv"
    
#     # Read the CSV
#     df = pd.read_csv(file_path)
    
#     # Extract brand, model, color from Smartphone column
#     extracted_data = df['Smartphone'].apply(extract_brand_model_color)
    
#     # Create new columns
#     df['brand_name'] = [x[0] for x in extracted_data]
#     df['model'] = [x[1] for x in extracted_data]
#     df['color'] = [x[2] for x in extracted_data]
    
#     # Drop the original Smartphone column
#     df = df.drop('Smartphone', axis=1)
    
#     # Reorder columns to put brand_name, model, color at the beginning
#     cols = ['brand_name', 'model', 'color'] + [col for col in df.columns if col not in ['brand_name', 'model', 'color']]
#     df = df[cols]
    
#     # Save the modified CSV
#     output_path = file_path.replace('.csv', '_modified.csv')
#     df.to_csv(output_path, index=False)
    
#     print(f"Amazon CSV modified and saved as: {output_path}")
#     print(f"Sample of extracted data:")
#     print(df[['brand_name', 'model', 'color']].head())
    
#     return output_path

def modify_iphone_csv():
    """Modify iPhone results CSV file"""
    file_path = r"e:\BenchSmart\Test Programs\kaggle_datasets\iphone_results.csv"
    
    # Read the CSV
    df = pd.read_csv(file_path)
    
    # Extract brand, model, color from Description column
    extracted_data = df['Description'].apply(extract_brand_model_color)
    
    # Create new columns
    df['brand_name'] = [x[0] for x in extracted_data]
    df['model'] = [x[1] for x in extracted_data]
    df['color'] = [x[2] for x in extracted_data]
    
    # Drop the original Description column
    df = df.drop('Description', axis=1)
    
    # Reorder columns to put brand_name, model, color at the beginning
    cols = ['brand_name', 'model', 'color'] + [col for col in df.columns if col not in ['brand_name', 'model', 'color']]
    df = df[cols]
    
    # Save the modified CSV
    output_path = file_path.replace('.csv', '_modified.csv')
    df.to_csv(output_path, index=False)
    
    print(f"iPhone CSV modified and saved as: {output_path}")
    print(f"Sample of extracted data:")
    print(df[['brand_name', 'model', 'color']].head())
    
    return output_path

def main():
    print("Modifying CSV files...")
    print("=" * 50)
    
    # Modify Amazon CSV
    # amazon_output = modify_amazon_csv()
    # print()
    
    # # Modify iPhone CSV  
    iphone_output = modify_iphone_csv()
    print()
    
    print("=" * 50)
    print("All CSV files have been successfully modified!")
    print(f"Modified files:")
    # print(f"1. {amazon_output}")
    print(f"{iphone_output}")

if __name__ == "__main__":
    main()

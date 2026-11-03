import {
  Select as AriaSelect,
  Button,
  Label,
  ListBox,
  ListBoxItem,
  Popover,
  SelectValue,
} from "react-aria-components";
import styles from "./Select.module.css";

interface Props {
  label: string;
  "aria-label"?: string;
  value: string;
  options: { id: string; name: string }[];
  onChange: (value: string) => void;
}

/** One choice, named in plain words; long option sets scroll in the popover. */
export function Select({
  label,
  "aria-label": ariaLabel,
  value,
  options,
  onChange,
}: Props) {
  return (
    <AriaSelect
      className={styles.select}
      aria-label={ariaLabel}
      selectedKey={value}
      onSelectionChange={(key) => {
        if (key !== null) onChange(String(key));
      }}
    >
      <Label className="micro-label">{label}</Label>
      <Button className={styles.trigger}>
        <SelectValue className={styles.value} />
        <span aria-hidden="true">▾</span>
      </Button>
      <Popover
        className={({ isEntering, isExiting }) =>
          `${styles.popover} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
        }
      >
        <ListBox className={styles.list} items={options}>
          {(option) => (
            <ListBoxItem
              id={option.id}
              textValue={option.name}
              className={styles.option}
            >
              {option.name}
            </ListBoxItem>
          )}
        </ListBox>
      </Popover>
    </AriaSelect>
  );
}

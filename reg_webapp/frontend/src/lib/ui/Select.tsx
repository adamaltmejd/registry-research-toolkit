import { useState } from "react";
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

interface Common {
  label: string;
  labelHidden?: boolean;
  "aria-label"?: string;
  placeholder?: string;
  options: { id: string; name: string }[];
}

type Props = Common &
  (
    | {
        selectionMode?: "single";
        /** `null` is no choice yet: the trigger shows `placeholder`. */
        value: string | null;
        onChange: (value: string) => void;
      }
    | {
        /** Several choices; none chosen shows `placeholder`. */
        selectionMode: "multiple";
        value: readonly string[];
        onChange: (value: string[]) => void;
      }
  );

/** One choice, or several, named in plain words; long option sets scroll in
 * the popover. */
export function Select(props: Props) {
  const {
    label,
    labelHidden = false,
    "aria-label": ariaLabel,
    placeholder,
    options,
  } = props;
  const [open, setOpen] = useState(false);
  const parts = (
    <>
      <Label className={labelHidden ? "visually-hidden" : "micro-label"}>
        {label}
      </Label>
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
    </>
  );
  if (props.selectionMode === "multiple") {
    const { onChange } = props;
    return (
      <AriaSelect
        className={styles.select}
        aria-label={ariaLabel}
        selectionMode="multiple"
        value={[...props.value]}
        placeholder={placeholder}
        // Each pick closes, as a single choice does: a pick can change the
        // page above the trigger (a finding it clears), and the popover
        // would stay where the trigger was.
        isOpen={open}
        onOpenChange={setOpen}
        onChange={(keys) => {
          setOpen(false);
          onChange(keys.map(String));
        }}
      >
        {parts}
      </AriaSelect>
    );
  }
  const { onChange } = props;
  return (
    <AriaSelect
      className={styles.select}
      aria-label={ariaLabel}
      selectedKey={props.value}
      placeholder={placeholder}
      onSelectionChange={(key) => {
        if (key !== null) onChange(String(key));
      }}
    >
      {parts}
    </AriaSelect>
  );
}

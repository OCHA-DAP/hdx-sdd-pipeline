from classifiers.pii_classifier import PIIClassifier
from classifiers.pii_reflection_classifier import PIIReflectionClassifier
from classifiers.non_pii_classifier import NonPIIClassifier

model_name = "gpt-4o-mini"

pii_detector = PIIClassifier(model_name)
pii_reflection = PIIReflectionClassifier(model_name)
non_pii = NonPIIClassifier(model_name)

# Example PII detection
res1 = pii_detector.classify("email_address", ["john@example.com", "jane@company.com"])
print(res1)

# Example PII sensitivity
res2 = pii_reflection.classify("email_address", "Column with email info", "EMAIL")
print(res2)

# Example Non-PII table classification
res3 = non_pii.classify("TABLE SCHEMA AND SAMPLE DATA", isp={"rules": "some_isp_rules"})
print(res3)
